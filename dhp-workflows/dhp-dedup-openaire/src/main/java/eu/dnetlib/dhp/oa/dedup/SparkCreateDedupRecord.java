package eu.dnetlib.dhp.oa.dedup;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;

import java.io.IOException;

public class SparkCreateDedupRecord extends AbstractSparkAction {

    private static final Log log = LogFactory.getLog(SparkCreateDedupRecord.class);

    public SparkCreateDedupRecord(ArgumentApplicationParser parser, SparkSession spark) throws Exception {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createDedupRecord_parameters.json")));
        parser.parseArgument(args);

        new SparkCreateSimRels(parser, getSparkSession(parser)).run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
    }

    @Override
    public void run(ISLookUpService isLookUpService) throws ISLookUpException, DocumentException, IOException {

        final String graphBasePath = parser.get("graphBasePath");
        final String isLookUpUrl = parser.get("isLookUpUrl");
        final String actionSetId = parser.get("actionSetId");
        final String workingPath = parser.get("workingPath");

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        for (DedupConfig dedupConf: getConfigurations(isLookUpService, actionSetId)) {
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
