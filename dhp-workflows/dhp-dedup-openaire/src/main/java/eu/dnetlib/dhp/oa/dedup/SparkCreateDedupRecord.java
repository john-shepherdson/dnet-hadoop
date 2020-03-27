package eu.dnetlib.dhp.oa.dedup;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.pace.config.DedupConfig;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;

public class SparkCreateDedupRecord {

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateDedupRecord.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createDedupRecord_parameters.json")));
        parser.parseArgument(args);

        new SparkCreateDedupRecord().run(parser);
    }

    private void run(ArgumentApplicationParser parser) throws ISLookUpException, DocumentException {

        final String graphBasePath = parser.get("graphBasePath");
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");
        final String workingPath = parser.get("workingPath");

        try (SparkSession spark = getSparkSession(parser)) {
            final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

            for (DedupConfig dedupConf: DedupUtility.getConfigurations(isLookUpUrl, actionSetId)) {
                String subEntity = dedupConf.getWf().getSubEntityValue();

                final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, subEntity);
                final String entityPath = DedupUtility.createEntityPath(graphBasePath, subEntity);
                final OafEntityType entityType = OafEntityType.valueOf(subEntity);
                final JavaRDD<OafEntity> dedupRecord =
                        DedupRecordFactory.createDedupRecord(sc, spark, mergeRelPath, entityPath, entityType, dedupConf);
                dedupRecord.map(r -> {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.writeValueAsString(r);
                }).saveAsTextFile(DedupUtility.createDedupRecordPath(workingPath, actionSetId, subEntity));
            }
        }
    }

    private static SparkSession getSparkSession(ArgumentApplicationParser parser) {
        SparkConf conf = new SparkConf();

        return SparkSession
                .builder()
                .appName(SparkCreateDedupRecord.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
    }
}

