package eu.dnetlib.dhp.oa.graph.dump.community;

import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class CommunitySplit implements Serializable {


    public void run(Boolean isSparkSessionManaged, String inputPath, String outputPath, CommunityMap communityMap) {
        SparkConf conf = new SparkConf();
        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    Utils.removeOutputDir(spark, outputPath);
                    execSplit(spark, inputPath, outputPath, communityMap.keySet());// , inputClazz);
                });
    }

    private static void execSplit(SparkSession spark, String inputPath, String outputPath,
                                  Set<String> communities) {// }, Class<R> inputClazz) {

        Dataset<CommunityResult> result = Utils
                .readPath(spark, inputPath + "/publication", CommunityResult.class)
                .union(Utils.readPath(spark, inputPath + "/dataset", CommunityResult.class))
                .union(Utils.readPath(spark, inputPath + "/orp", CommunityResult.class))
                .union(Utils.readPath(spark, inputPath + "/software", CommunityResult.class));

        communities
                .stream()
                .forEach(c -> printResult(c, result, outputPath));

    }

    private static void printResult(String c, Dataset<CommunityResult> result, String outputPath) {
        Dataset<CommunityResult> community_products = result
                .filter(r -> containsCommunity(r, c));

        try{
            community_products.first();
            community_products
                    .repartition(1)
                    .write()
                    .option("compression", "gzip")
                    .mode(SaveMode.Overwrite)
                    .json(outputPath + "/" + c);
        }catch(Exception e){

        }

    }

    private static boolean containsCommunity(CommunityResult r, String c) {
        if (Optional.ofNullable(r.getContext()).isPresent()) {
            return r
                    .getContext()
                    .stream()
                    .filter(con -> con.getCode().equals(c))
                    .collect(Collectors.toList())
                    .size() > 0;
        }
        return false;
    }
}
