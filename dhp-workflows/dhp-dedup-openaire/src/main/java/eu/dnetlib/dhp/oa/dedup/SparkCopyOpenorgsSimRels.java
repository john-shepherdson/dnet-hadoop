
package eu.dnetlib.dhp.oa.dedup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.pace.config.DedupConfig;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

//copy simrels (verified) from relation to the workdir in order to make them available for the deduplication
public class SparkCopyOpenorgsSimRels extends AbstractSparkAction {
    private static final Logger log = LoggerFactory.getLogger(SparkCopyOpenorgsMergeRels.class);

    public SparkCopyOpenorgsSimRels(ArgumentApplicationParser parser, SparkSession spark) {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils
                        .toString(
                                SparkCopyOpenorgsSimRels.class
                                        .getResourceAsStream(
                                                "/eu/dnetlib/dhp/oa/dedup/copyOpenorgsMergeRels_parameters.json")));
        parser.parseArgument(args);

        SparkConf conf = new SparkConf();
        new SparkCopyOpenorgsSimRels(parser, getSparkSession(conf))
                .run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
    }

    @Override
    public void run(ISLookUpService isLookUpService)
            throws DocumentException, IOException, ISLookUpException {

        // read oozie parameters
        final String graphBasePath = parser.get("graphBasePath");
        final String actionSetId = parser.get("actionSetId");
        final String workingPath = parser.get("workingPath");
        final int numPartitions = Optional
                .ofNullable(parser.get("numPartitions"))
                .map(Integer::valueOf)
                .orElse(NUM_PARTITIONS);

        log.info("numPartitions: '{}'", numPartitions);
        log.info("graphBasePath: '{}'", graphBasePath);
        log.info("actionSetId:   '{}'", actionSetId);
        log.info("workingPath:   '{}'", workingPath);

        log.info("Copying OpenOrgs SimRels");

        final String outputPath = DedupUtility.createSimRelPath(workingPath, actionSetId, "organization");

        removeOutputDir(spark, outputPath);

        final String relationPath = DedupUtility.createEntityPath(graphBasePath, "relation");

        JavaRDD<Relation> rawRels = spark
                .read()
                .textFile(relationPath)
                .map(patchRelFn(), Encoders.bean(Relation.class))
                .toJavaRDD()
                .filter(this::isOpenorgs)
                .filter(this::filterOpenorgsRels);

        save(spark.createDataset(rawRels.rdd(),Encoders.bean(Relation.class)), outputPath, SaveMode.Append);
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

    private boolean filterOpenorgsRels(Relation rel) {

        if (rel.getRelClass().equals("isSimilarTo") && rel.getRelType().equals("organizationOrganization") && rel.getSubRelType().equals("dedup"))
            return true;
        return false;
    }

    private boolean isOpenorgs(Relation rel) {

        if (rel.getCollectedfrom() != null) {
            for (KeyValue k: rel.getCollectedfrom()) {
                if (k.getValue().equals("OpenOrgs Database")) {
                    return true;
                }
            }
        }
        return false;
    }
}

