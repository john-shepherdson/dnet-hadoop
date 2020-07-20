package eu.dnetlib.dhp.oa.graph.dump.community;

import eu.dnetlib.dhp.oa.graph.dump.ResultMapper;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.oaf.Context;
import eu.dnetlib.dhp.schema.oaf.Result;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class DumpProducts implements Serializable {

    public void run(Boolean isSparkSessionManaged, String inputPath, String outputPath, CommunityMap communityMap, Class<? extends Result> inputClazz, boolean graph) {

        SparkConf conf = new SparkConf();

        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    Utils.removeOutputDir(spark, outputPath);
                    execDump(spark, inputPath, outputPath, communityMap, inputClazz, graph);// , dumpClazz);

                });
    }

    public static <I extends Result, O extends eu.dnetlib.dhp.schema.dump.oaf.Result> void execDump(SparkSession spark,
                                                                                                    String inputPath,
                                                                                                    String outputPath,
                                                                                                    CommunityMap communityMap,
                                                                                                    Class<I> inputClazz,
                                                                                                    boolean graph) {

        Dataset<I> tmp = Utils.readPath(spark, inputPath, inputClazz);

        tmp
                .map(value -> execMap(value, communityMap, graph), Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.Result.class))
                .filter(Objects::nonNull)
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression", "gzip")
                .json(outputPath);

    }

    private static <I extends Result> eu.dnetlib.dhp.schema.dump.oaf.Result execMap(I value,
                                                                                    CommunityMap communityMap,
                                                                                    boolean graph) {

        if (!graph) {
            Set<String> communities = communityMap.keySet();

            Optional<List<Context>> inputContext = Optional.ofNullable(value.getContext());
            if (!inputContext.isPresent()) {
                return null;
            }
            List<String> toDumpFor = inputContext.get().stream().map(c -> {
                if (communities.contains(c.getId())) {
                    return c.getId();
                }
                if (c.getId().contains("::") && communities.contains(c.getId().substring(0, c.getId().indexOf("::")))) {
                    return c.getId().substring(0, 3);
                }
                return null;
            }).filter(Objects::nonNull).collect(Collectors.toList());
            if (toDumpFor.size() == 0) {
                return null;
            }
        }
            return ResultMapper.map(value, communityMap);

    }
}
