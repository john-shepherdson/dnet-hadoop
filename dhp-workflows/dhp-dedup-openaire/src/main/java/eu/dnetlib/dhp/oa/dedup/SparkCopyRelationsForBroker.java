package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

// copies relations needed by the broker to calculate its events.
// (the relations are supposed to be copied before the propagation of the relations,
// otherwise they would disappear due to the deletion of the deletedbyinference)
public class SparkCopyRelationsForBroker extends AbstractSparkAction{
    private static final Logger log = LoggerFactory.getLogger(SparkCopyRelationsNoOpenorgs.class);

    public SparkCopyRelationsForBroker(ArgumentApplicationParser parser, SparkSession spark) {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils
                        .toString(
                                SparkCopyRelationsNoOpenorgs.class
                                        .getResourceAsStream(
                                                "/eu/dnetlib/dhp/oa/dedup/copyRelationsForBroker_parameters.json")));
        parser.parseArgument(args);

        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(ModelSupport.getOafModelClasses());

        new SparkCopyRelationsForBroker(parser, getSparkSession(conf))
                .run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
    }

    public void run(ISLookUpService isLookUpService) throws IOException {

        final String graphBasePath = parser.get("graphBasePath");
        final String workingPath = parser.get("workingPath");
        final String relationsOutputPath = parser.get("relationsOutputPath");

        log.info("graphBasePath:       '{}'", graphBasePath);
        log.info("workingPath:         '{}'", workingPath);
        log.info("relationsOutputPath: '{}'", relationsOutputPath);

        final String relationPath = DedupUtility.createEntityPath(graphBasePath, "relation");

        JavaRDD<Relation> relationsForBroker = spark
                .read()
                .schema(Encoders.bean(Relation.class).schema())
                .json(relationPath)
                .as(Encoders.bean(Relation.class))
                .map(patchRelFn(), Encoders.bean(Relation.class))
                .toJavaRDD()
                .filter(this::isNeededByBrokerRel);

        if (log.isDebugEnabled()) {
            log.debug("Number of relations needed by the broker collected: {}", relationsForBroker.count());
        }

        save(spark.createDataset(relationsForBroker.rdd(), Encoders.bean(Relation.class)), relationsOutputPath, SaveMode.Overwrite);
    }
}
