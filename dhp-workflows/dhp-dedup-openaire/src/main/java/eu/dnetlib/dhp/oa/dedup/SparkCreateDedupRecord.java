package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SparkCreateDedupRecord extends AbstractSparkAction {

    private static final Logger log = LoggerFactory.getLogger(SparkCreateDedupRecord.class);

    public SparkCreateDedupRecord(ArgumentApplicationParser parser, SparkSession spark) {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createDedupRecord_parameters.json")));
        parser.parseArgument(args);

        new SparkCreateDedupRecord(parser, getSparkSession(parser)).run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
    }

    @Override
    public void run(ISLookUpService isLookUpService) throws ISLookUpException, DocumentException, IOException {

        final String graphBasePath = parser.get("graphBasePath");
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");
        final String workingPath = parser.get("workingPath");

        log.info("graphBasePath: '{}'", graphBasePath);
        log.info("isLookUpUrl:   '{}'", isLookUpUrl);
        log.info("actionSetId:   '{}'", actionSetId);
        log.info("workingPath:   '{}'", workingPath);

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        for (DedupConfig dedupConf: getConfigurations(isLookUpService, actionSetId)) {
            String subEntity = dedupConf.getWf().getSubEntityValue();
            log.info("Creating deduprecords for: '{}'", subEntity);

            final String outputPath = DedupUtility.createDedupRecordPath(workingPath, actionSetId, subEntity);
            removeOutputDir(spark, outputPath);

            final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, subEntity);
            final String entityPath = DedupUtility.createEntityPath(graphBasePath, subEntity);
            final OafEntityType entityType = OafEntityType.valueOf(subEntity);
            final JavaRDD<OafEntity> dedupRecord =
                    DedupRecordFactory.createDedupRecord(sc, spark, mergeRelPath, entityPath, entityType, dedupConf);

            dedupRecord.map(r -> OBJECT_MAPPER.writeValueAsString(r)).saveAsTextFile(outputPath);
        }

    }

}
