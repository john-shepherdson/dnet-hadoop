package eu.dnetlib.dhp.oa.graph.dump;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

import eu.dnetlib.dhp.schema.dump.oaf.Result;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class SparkSplitForCommunity implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(SparkSplitForCommunity.class);


    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils
                .toString(
                        SparkSplitForCommunity.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/oa/graph/dump/split_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);


        final String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        final String resultClassName = parser.get("resultTableName");
        log.info("resultTableName: {}", resultClassName);

        final String isLookUpUrl = parser.get("isLookUpUrl");
        log.info("isLookUpUrl: {}", isLookUpUrl);


        Class<? extends Result> inputClazz = (Class<? extends Result>) Class.forName(resultClassName);

        SparkConf conf = new SparkConf();

        Map<String,String>
                communityMap = QueryInformationSystem.getCommunityMap(isLookUpUrl);


        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    Utils.removeOutputDir(spark, outputPath);
                    execSplit(spark, inputPath, outputPath , communityMap.keySet(), inputClazz);
                });
    }

    private static <R extends Result> void execSplit(SparkSession spark, String inputPath, String outputPath, Set<String> communities
            , Class<R> inputClazz) {

        Dataset<R> result = Utils.readPath(spark, inputPath, inputClazz);

        communities.stream()
                .forEach(c -> printResult(c, result, outputPath));

    }

    private static <R extends Result> void printResult(String c, Dataset<R> result, String outputPath) {
        result.filter(r -> containsCommunity(r, c))
                .write()
                .option("compression","gzip")
                .mode(SaveMode.Append)
                .json(outputPath + "/" + c);
    }

    private static <R extends Result> boolean containsCommunity(R r, String c) {
        if(Optional.ofNullable(r.getContext()).isPresent()) {
            return r.getContext().stream().filter(con -> con.getCode().equals(c)).collect(Collectors.toList()).size() > 0;
        }
        return false;
    }
}
