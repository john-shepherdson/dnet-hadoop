package eu.dnetlib.dhp;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;



public class SparkBulkTagJob {

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkBulkTagJob.class.getResourceAsStream("/eu/dnetlib/dhp/input_bulktag_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkBulkTagJob.class.getSimpleName())
                .master(parser.get("master"))
                .enableHiveSupport()
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/bulktagging";

        final ResultTagger resultTagger = new ResultTagger();
        ProtoMap protoMappingParams = new Gson().fromJson(parser.get("mappingProto"),ProtoMap.class);;

        File directory = new File(outputPath);

        if (!directory.exists()) {
            directory.mkdirs();
        }

        CommunityConfiguration cc = QueryInformationSystem.getCommunityConfiguration(parser.get("isLookupUrl"));


        sc.sequenceFile(inputPath + "/publication", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Publication.class))
                .map(p -> resultTagger.enrichContextCriteria(p, cc, protoMappingParams))
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath+"/publication");
        sc.sequenceFile(inputPath + "/dataset", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Dataset.class))
                .map(p -> resultTagger.enrichContextCriteria(p, cc, protoMappingParams))
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath+"/dataset");
        sc.sequenceFile(inputPath + "/software", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Software.class))
                .map(p -> resultTagger.enrichContextCriteria(p, cc, protoMappingParams))
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath+"/software");
        sc.sequenceFile(inputPath + "/otherresearchproduct", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), OtherResearchProduct.class))
                .map(p -> resultTagger.enrichContextCriteria(p, cc, protoMappingParams))
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath+"/otherresearchproduct");



    }
}
