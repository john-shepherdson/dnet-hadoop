package eu.dnetlib.dhp.resulttocommunityfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.resulttocommunityfromorganization.ResultCommunityList;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class PrepareResultCommunitySetStep2 {
    private static final Logger log = LoggerFactory.getLogger(PrepareResultCommunitySetStep2.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        String jsonConfiguration =
                IOUtils.toString(
                        PrepareResultCommunitySetStep2.class.getResourceAsStream(
                                "/eu/dnetlib/dhp/resulttocommunityfromsemrel/input_preparecommunitytoresult2_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

        parser.parseArgument(args);

        Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        SparkConf conf = new SparkConf();

        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    if (isTest(parser)) {
                        removeOutputDir(spark, outputPath);
                    }
                    mergeInfo(spark, inputPath, outputPath);
                });
    }

    private static void mergeInfo(SparkSession spark, String inputPath, String outputPath) {

        Dataset<ResultCommunityList> resultOrcidAssocCommunityList =
                readResultCommunityList(spark, inputPath + "/publication")
                        .union(readResultCommunityList(spark, inputPath + "/dataset"))
                        .union(readResultCommunityList(spark, inputPath + "/otherresearchproduct"))
                        .union(readResultCommunityList(spark, inputPath + "/software"));

        resultOrcidAssocCommunityList
                .toJavaRDD()
                .mapToPair(r -> new Tuple2<>(r.getResultId(), r))
                .reduceByKey(
                        (a, b) -> {
                            if (a == null) {
                                return b;
                            }
                            if (b == null) {
                                return a;
                            }
                            Set<String> community_set = new HashSet<>();

                            a.getCommunityList().stream().forEach(aa -> community_set.add(aa));

                            b.getCommunityList().stream()
                                    .forEach(
                                            aa -> {
                                                if (!community_set.contains(aa)) {
                                                    a.getCommunityList().add(aa);
                                                    community_set.add(aa);
                                                }
                                            });
                            return a;
                        })
                .map(c -> c._2())
                .map(r -> OBJECT_MAPPER.writeValueAsString(r))
                .saveAsTextFile(outputPath, GzipCodec.class);
    }

    private static Dataset<ResultCommunityList> readResultCommunityList(
            SparkSession spark, String relationPath) {
        return spark.read()
                .textFile(relationPath)
                .map(
                        value -> OBJECT_MAPPER.readValue(value, ResultCommunityList.class),
                        Encoders.bean(ResultCommunityList.class));
    }
}
