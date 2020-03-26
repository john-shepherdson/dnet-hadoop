package eu.dnetlib.dhp.graph;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class SparkGraphImporterJobTest {

    private static final long MAX = 1000L;

    @Disabled("must be parametrized to run locally")
    public void testImport(@TempDir Path outPath) throws Exception {
        SparkGraphImporterJob.main(new String[] {
                "-mt", "local[*]",
                "-s", getClass().getResource("/eu/dnetlib/dhp/graph/sample").getPath(),
                "-h", "",
                "-db", "test"
        });

        countEntities(outPath.toString()).forEach(t -> {
            System.out.println(t);
            Assertions.assertEquals(MAX, t._2().longValue(), String.format("mapped %s must be %s", t._1(), MAX));
        });
    }

    public static List<Tuple2<String, Long>> countEntities(final String inputPath) {

        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkGraphImporterJobTest.class.getSimpleName())
                .master("local[*]")
                .getOrCreate();
        //final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        return GraphMappingUtils.types.entrySet()
                .stream()
                .map(entry -> {
                    final Long count = spark.read().load(inputPath + "/" + entry.getKey()).as(Encoders.bean(entry.getValue())).count();
                    return new Tuple2<String, Long>(entry.getKey(), count);
                })
                .collect(Collectors.toList());
    }
}
