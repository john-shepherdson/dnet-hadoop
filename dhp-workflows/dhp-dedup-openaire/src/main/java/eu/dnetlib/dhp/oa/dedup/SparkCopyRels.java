package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

//copy simrels (verified) from relation to the workdir in order to make them available for the deduplication
public class SparkCopyRels extends AbstractSparkAction{
    private static final Logger log = LoggerFactory.getLogger(SparkCopyRels.class);

    public SparkCopyRels(ArgumentApplicationParser parser, SparkSession spark) {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils
                        .toString(
                                SparkCopyRels.class
                                        .getResourceAsStream(
                                                "/eu/dnetlib/dhp/oa/dedup/copyRels_parameters.json")));
        parser.parseArgument(args);

        SparkConf conf = new SparkConf();
        new SparkCopyRels(parser, getSparkSession(conf))
                .run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
    }

    @Override
    public void run(ISLookUpService isLookUpService)
            throws DocumentException, IOException, ISLookUpException {

        // read oozie parameters
        final String graphBasePath = parser.get("graphBasePath");
        final String actionSetId = parser.get("actionSetId");
        final String workingPath = parser.get("workingPath");
        final String destination = parser.get("destination");
        final String entity = parser.get("entityType");
        final int numPartitions = Optional
                .ofNullable(parser.get("numPartitions"))
                .map(Integer::valueOf)
                .orElse(NUM_PARTITIONS);

        log.info("numPartitions: '{}'", numPartitions);
        log.info("graphBasePath: '{}'", graphBasePath);
        log.info("actionSetId:   '{}'", actionSetId);
        log.info("workingPath:   '{}'", workingPath);
        log.info("entity:        '{}'", entity);

        log.info("Copying " + destination + " for: '{}'", entity);

        final String outputPath;
        if (destination.contains("mergerel")) {
            outputPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, entity);
        }
        else {
            outputPath = DedupUtility.createSimRelPath(workingPath, actionSetId, entity);
        }

        removeOutputDir(spark, outputPath);

        final String relationPath = DedupUtility.createEntityPath(graphBasePath, "relation");

        JavaRDD<Relation> simRels =
                spark.read()
                        .textFile(relationPath)
                        .map(patchRelFn(), Encoders.bean(Relation.class))
                        .toJavaRDD()
                        .filter(r -> filterRels(r, entity));

        simRels.saveAsTextFile(outputPath);
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
