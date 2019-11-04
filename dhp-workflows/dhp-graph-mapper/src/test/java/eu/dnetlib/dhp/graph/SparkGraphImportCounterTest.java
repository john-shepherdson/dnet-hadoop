package eu.dnetlib.dhp.graph;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;

public class SparkGraphImportCounterTest {

    public static List<Tuple2<String, Long>> countEntities(final String inputPath) throws Exception {

        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkGraphImportCounterTest.class.getSimpleName())
                .master("local[*]")
                .getOrCreate();
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        return GraphMappingUtils.types.entrySet()
                .stream()
                .map(entry -> {
                    final Long count = spark.read().load(inputPath + "/" + entry.getKey()).as(Encoders.bean(entry.getValue())).count();
                    return new Tuple2<String, Long>(entry.getKey(), count);
                })
                .collect(Collectors.toList());
    }

}
