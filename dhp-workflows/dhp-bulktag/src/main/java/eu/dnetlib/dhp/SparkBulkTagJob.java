package eu.dnetlib.dhp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.community.CommunityConfiguration;
import eu.dnetlib.dhp.community.ProtoMap;
import eu.dnetlib.dhp.community.QueryInformationSystem;
import eu.dnetlib.dhp.community.ResultTagger;
import eu.dnetlib.dhp.schema.oaf.*;
import java.io.File;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkBulkTagJob {

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser =
                new ArgumentApplicationParser(
                        IOUtils.toString(
                                SparkBulkTagJob.class.getResourceAsStream(
                                        "/eu/dnetlib/dhp/input_bulktag_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark =
                SparkSession.builder()
                        .appName(SparkBulkTagJob.class.getSimpleName())
                        .master(parser.get("master"))
                        .enableHiveSupport()
                        .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/bulktagging";

        final ResultTagger resultTagger = new ResultTagger();
        ProtoMap protoMappingParams =
                new Gson().fromJson(parser.get("mappingProto"), ProtoMap.class);
        ;

        File directory = new File(outputPath);

        if (!directory.exists()) {
            directory.mkdirs();
        }

        CommunityConfiguration cc =
                QueryInformationSystem.getCommunityConfiguration(parser.get("isLookupUrl"));

        sc.textFile(inputPath + "/publication")
                .map(item -> new ObjectMapper().readValue(item, Publication.class))
                .map(p -> resultTagger.enrichContextCriteria(p, cc, protoMappingParams))
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath + "/publication");
        sc.textFile(inputPath + "/dataset")
                .map(item -> new ObjectMapper().readValue(item, Dataset.class))
                .map(p -> resultTagger.enrichContextCriteria(p, cc, protoMappingParams))
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath + "/dataset");
        sc.textFile(inputPath + "/software")
                .map(item -> new ObjectMapper().readValue(item, Software.class))
                .map(p -> resultTagger.enrichContextCriteria(p, cc, protoMappingParams))
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath + "/software");
        sc.textFile(inputPath + "/otherresearchproduct")
                .map(item -> new ObjectMapper().readValue(item, OtherResearchProduct.class))
                .map(p -> resultTagger.enrichContextCriteria(p, cc, protoMappingParams))
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath + "/otherresearchproduct");
    }
}
