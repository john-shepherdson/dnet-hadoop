package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
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

        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(ModelSupport.getOafModelClasses());

        new SparkCreateDedupRecord(parser, getSparkSession(conf))
                .run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
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

        for (DedupConfig dedupConf: getConfigurations(isLookUpService, actionSetId)) {
            String subEntity = dedupConf.getWf().getSubEntityValue();
            log.info("Creating deduprecords for: '{}'", subEntity);

            final String outputPath = DedupUtility.createDedupRecordPath(workingPath, actionSetId, subEntity);
            removeOutputDir(spark, outputPath);

            final String mergeRelPath = DedupUtility.createMergeRelPath(workingPath, actionSetId, subEntity);
            final String entityPath = DedupUtility.createEntityPath(graphBasePath, subEntity);

            Class<OafEntity> clazz = ModelSupport.entityTypes.get(EntityType.valueOf(subEntity));

            DedupRecordFactory.createDedupRecord(spark, mergeRelPath, entityPath, clazz)
                    .map((MapFunction<OafEntity, String>) value -> OBJECT_MAPPER.writeValueAsString(value), Encoders.STRING())
                .write()
                .mode(SaveMode.Overwrite)
                .json(outputPath);
        }

    }

}
