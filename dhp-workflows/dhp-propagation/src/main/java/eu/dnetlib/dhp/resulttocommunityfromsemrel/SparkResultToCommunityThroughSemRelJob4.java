package eu.dnetlib.dhp.resulttocommunityfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ximpleware.extended.xpath.parser;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.resulttocommunityfromorganization.ResultCommunityList;
import eu.dnetlib.dhp.schema.oaf.*;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkResultToCommunityThroughSemRelJob4 {

    private static final Logger log =
            LoggerFactory.getLogger(SparkResultToCommunityThroughSemRelJob4.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        String jsonConfiguration =
                IOUtils.toString(
                        SparkResultToCommunityThroughSemRelJob4.class.getResourceAsStream(
                                "/eu/dnetlib/dhp/resulttocommunityfromsemrel/input_communitytoresult_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

        parser.parseArgument(args);

        Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        final String preparedInfoPath = parser.get("preparedInfoPath");
        log.info("preparedInfoPath: {}", preparedInfoPath);

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

        final String resultClassName = parser.get("resultTableName");
        log.info("resultTableName: {}", resultClassName);

        final Boolean saveGraph =
                Optional.ofNullable(parser.get("saveGraph"))
                        .map(Boolean::valueOf)
                        .orElse(Boolean.TRUE);
        log.info("saveGraph: {}", saveGraph);

        Class<? extends Result> resultClazz =
                (Class<? extends Result>) Class.forName(resultClassName);

        runWithSparkHiveSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    if (isTest(parser)) {
                        removeOutputDir(spark, outputPath);
                    }
                    if (saveGraph) {
                        execPropagation(
                                spark, inputPath, outputPath, preparedInfoPath, resultClazz);
                    }
                });
    }

    private static <R extends Result> void execPropagation(
            SparkSession spark,
            String inputPath,
            String outputPath,
            String preparedInfoPath,
            Class<R> resultClazz) {

        org.apache.spark.sql.Dataset<ResultCommunityList> possibleUpdates =
                readResultCommunityList(spark, preparedInfoPath);
        org.apache.spark.sql.Dataset<R> result = readPathEntity(spark, inputPath, resultClazz);

        result.joinWith(
                        possibleUpdates,
                        result.col("id").equalTo(possibleUpdates.col("resultId")),
                        "left_outer")
                .map(
                        value -> {
                            R ret = value._1();
                            Optional<ResultCommunityList> rcl = Optional.ofNullable(value._2());
                            if (rcl.isPresent()) {
                                Set<String> context_set = new HashSet<>();
                                ret.getContext().stream().forEach(c -> context_set.add(c.getId()));
                                List<Context> contextList =
                                        rcl.get().getCommunityList().stream()
                                                .map(
                                                        c -> {
                                                            if (!context_set.contains(c)) {
                                                                Context newContext = new Context();
                                                                newContext.setId(c);
                                                                newContext.setDataInfo(
                                                                        Arrays.asList(
                                                                                getDataInfo(
                                                                                        PROPAGATION_DATA_INFO_TYPE,
                                                                                        PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID,
                                                                                        PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME)));
                                                                return newContext;
                                                            }
                                                            return null;
                                                        })
                                                .filter(c -> c != null)
                                                .collect(Collectors.toList());
                                Result r = new Result();
                                r.setId(ret.getId());
                                r.setContext(contextList);
                                ret.mergeFrom(r);
                            }

                            return ret;
                        },
                        Encoders.bean(resultClazz))
                .toJSON()
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression", "gzip")
                .text(outputPath);
    }
}
