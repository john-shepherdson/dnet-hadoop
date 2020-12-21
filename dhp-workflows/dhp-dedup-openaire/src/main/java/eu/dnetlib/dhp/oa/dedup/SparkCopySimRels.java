package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

//copy simrels (verified) from relation to the workdir in order to make them available for the deduplication
public class SparkCopySimRels extends AbstractSparkAction{
    private static final Logger log = LoggerFactory.getLogger(SparkCopySimRels.class);

    public SparkCopySimRels(ArgumentApplicationParser parser, SparkSession spark) {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils
                        .toString(
                                SparkCreateSimRels.class
                                        .getResourceAsStream(
                                                "/eu/dnetlib/dhp/oa/dedup/createSimRels_parameters.json")));
        parser.parseArgument(args);

        SparkConf conf = new SparkConf();
        new SparkCreateSimRels(parser, getSparkSession(conf))
                .run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
    }

    @Override
    public void run(ISLookUpService isLookUpService)
            throws DocumentException, IOException, ISLookUpException {

        // read oozie parameters
        final String graphBasePath = parser.get("graphBasePath");
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");
        final String workingPath = parser.get("workingPath");
        final int numPartitions = Optional
                .ofNullable(parser.get("numPartitions"))
                .map(Integer::valueOf)
                .orElse(NUM_PARTITIONS);

        log.info("numPartitions: '{}'", numPartitions);
        log.info("graphBasePath: '{}'", graphBasePath);
        log.info("isLookUpUrl:   '{}'", isLookUpUrl);
        log.info("actionSetId:   '{}'", actionSetId);
        log.info("workingPath:   '{}'", workingPath);

        // for each dedup configuration
        for (DedupConfig dedupConf : getConfigurations(isLookUpService, actionSetId)) {

            final String entity = dedupConf.getWf().getEntityType();
            final String subEntity = dedupConf.getWf().getSubEntityValue();
            log.info("Copying simrels for: '{}'", subEntity);

            final String outputPath = DedupUtility.createSimRelPath(workingPath, actionSetId, subEntity);
            removeOutputDir(spark, outputPath);

            final String relationPath = DedupUtility.createEntityPath(graphBasePath, "relation");

            JavaRDD<Relation> simRels = spark.read().textFile(relationPath).map(patchRelFn(), Encoders.bean(Relation.class)).toJavaRDD().filter(r -> filterRels(r, entity));

            simRels.saveAsTextFile(outputPath);
        }
    }

    private static MapFunction<String, Relation> patchRelFn() {
        return value -> {
            final Relation rel = OBJECT_MAPPER.readValue(value, Relation.class);
            if (rel.getDataInfo() == null) {
                rel.setDataInfo(new DataInfo());
            }
            return rel;
        };
    }

    private boolean filterRels(Relation rel, String entityType) {

        switch(entityType) {
            case "result":
                if (rel.getRelClass().equals("isSimilarTo") && rel.getRelType().equals("resultResult") && rel.getSubRelType().equals("dedup"))
                    return true;
                break;
            case "organization":
                if (rel.getRelClass().equals("isSimilarTo") && rel.getRelType().equals("organizationOrganization") && rel.getSubRelType().equals("dedup"))
                    return true;
                break;
            default:
                return false;
        }
        return false;
    }
}
