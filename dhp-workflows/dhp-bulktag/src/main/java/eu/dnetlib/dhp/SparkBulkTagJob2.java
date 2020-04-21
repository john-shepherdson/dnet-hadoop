package eu.dnetlib.dhp;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.community.*;
import eu.dnetlib.dhp.schema.oaf.*;
import java.util.Optional;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkBulkTagJob2 {

    private static final Logger log = LoggerFactory.getLogger(SparkBulkTagJob2.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String jsonConfiguration =
                IOUtils.toString(
                        SparkBulkTagJob2.class.getResourceAsStream(
                                "/eu/dnetlib/dhp/input_bulktag_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

        parser.parseArgument(args);

        Boolean isSparkSessionManaged =
                Optional.ofNullable(parser.get("isSparkSessionManaged"))
                        .map(Boolean::valueOf)
                        .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        Boolean isTest =
                Optional.ofNullable(parser.get("isTest"))
                        .map(Boolean::valueOf)
                        .orElse(Boolean.FALSE);
        log.info("isTest: {} ", isTest);

        final String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        ProtoMap protoMappingParams = new Gson().fromJson(parser.get("protoMap"), ProtoMap.class);
        ;
        log.info("protoMap: {}", new Gson().toJson(protoMappingParams));

        final String resultClassName = parser.get("resultTableName");
        log.info("resultTableName: {}", resultClassName);

        final Boolean saveGraph =
                Optional.ofNullable(parser.get("saveGraph"))
                        .map(Boolean::valueOf)
                        .orElse(Boolean.TRUE);
        log.info("saveGraph: {}", saveGraph);

        Class<? extends Result> resultClazz =
                (Class<? extends Result>) Class.forName(resultClassName);

        SparkConf conf = new SparkConf();
        CommunityConfiguration cc;

        String taggingConf = parser.get("taggingConf");

        if (isTest) {
            cc = CommunityConfigurationFactory.fromJson(taggingConf);
        } else {
            cc = QueryInformationSystem.getCommunityConfiguration(parser.get("isLookupUrl"));
        }

        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    execBulkTag(spark, inputPath, outputPath, protoMappingParams, resultClazz, cc);
                });

        //        runWithSparkSession(conf, isSparkSessionManaged,
        //                spark -> {
        //                    if(isTest(parser)) {
        //                        removeOutputDir(spark, outputPath);
        //                    }
        //                    if(saveGraph)
        //                        execPropagation(spark, possibleUpdates, inputPath, outputPath,
        // resultClazz);
        //                });
        //
        //
        //
        //
        //
        //
        //        sc.textFile(inputPath + "/publication")
        //                .map(item -> new ObjectMapper().readValue(item, Publication.class))
        //                .map(p -> resultTagger.enrichContextCriteria(p, cc, protoMappingParams))
        //                .map(p -> new ObjectMapper().writeValueAsString(p))
        //                .saveAsTextFile(outputPath+"/publication");
        //        sc.textFile(inputPath + "/dataset")
        //                .map(item -> new ObjectMapper().readValue(item, Dataset.class))
        //                .map(p -> resultTagger.enrichContextCriteria(p, cc, protoMappingParams))
        //                .map(p -> new ObjectMapper().writeValueAsString(p))
        //                .saveAsTextFile(outputPath+"/dataset");
        //        sc.textFile(inputPath + "/software")
        //                .map(item -> new ObjectMapper().readValue(item, Software.class))
        //                .map(p -> resultTagger.enrichContextCriteria(p, cc, protoMappingParams))
        //                .map(p -> new ObjectMapper().writeValueAsString(p))
        //                .saveAsTextFile(outputPath+"/software");
        //        sc.textFile(inputPath + "/otherresearchproduct")
        //                .map(item -> new ObjectMapper().readValue(item,
        // OtherResearchProduct.class))
        //                .map(p -> resultTagger.enrichContextCriteria(p, cc, protoMappingParams))
        //                .map(p -> new ObjectMapper().writeValueAsString(p))
        //                .saveAsTextFile(outputPath+"/otherresearchproduct");
        //

    }

    private static <R extends Result> void execBulkTag(
            SparkSession spark,
            String inputPath,
            String outputPath,
            ProtoMap protoMappingParams,
            Class<R> resultClazz,
            CommunityConfiguration communityConfiguration) {

        ResultTagger resultTagger = new ResultTagger();
        Dataset<R> result = readPathEntity(spark, inputPath, resultClazz);
        result.map(
                        value ->
                                resultTagger.enrichContextCriteria(
                                        value, communityConfiguration, protoMappingParams),
                        Encoders.bean(resultClazz))
                .toJSON()
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression", "gzip")
                .text(outputPath);
    }

    private static <R extends Result> org.apache.spark.sql.Dataset<R> readPathEntity(
            SparkSession spark, String inputEntityPath, Class<R> resultClazz) {

        return spark.read()
                .textFile(inputEntityPath)
                .map(
                        (MapFunction<String, R>)
                                value -> OBJECT_MAPPER.readValue(value, resultClazz),
                        Encoders.bean(resultClazz));
    }
}
