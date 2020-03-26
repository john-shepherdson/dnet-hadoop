package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.graph.openaire.GraphMappingUtils;
import eu.dnetlib.dhp.graph.openaire.SparkGraphImporterJob;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

public class SparkGraphImporterJobTest {

    private final static String TEST_DB_NAME = "test";

    @Test
    public void testImport(@TempDir Path outPath) {
        try(SparkSession spark = testSparkSession(outPath.toString())) {

            new SparkGraphImporterJob().runWith(
                    spark,
                    getClass().getResource("/eu/dnetlib/dhp/graph/sample").getPath(),
                    TEST_DB_NAME);

            GraphMappingUtils.types.forEach((name, clazz) -> {
                final long count = spark.read().table(TEST_DB_NAME + "." + name).count();
                if (name.equals("relation")) {
                    Assertions.assertEquals(100, count, String.format("%s should be 100", name));
                } else {
                    Assertions.assertEquals(10, count, String.format("%s should be 10", name));
                }
            });
        }
    }

    private SparkSession testSparkSession(final String inputPath) {
        SparkConf conf = new SparkConf();

        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("hive.metastore.warehouse.dir", inputPath + "/warehouse");
        conf.set("spark.sql.warehouse.dir", inputPath);
        conf.set("javax.jdo.option.ConnectionURL", String.format("jdbc:derby:;databaseName=%s/junit_metastore_db;create=true", inputPath));
        conf.set("spark.ui.enabled", "false");

        return SparkSession
                .builder()
                .appName(SparkGraphImporterJobTest.class.getSimpleName())
                .master("local[*]")
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
    }

}
