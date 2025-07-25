package eu.dnetlib.dhp.actionmanager.affro;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static eu.dnetlib.dhp.actionmanager.affro.Constants.AFFILIATION_STRING_SCHEMA;

public class PrepareDatasetTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static SparkSession spark;

    private static Path workingDir;
    private static final String ID_PREFIX = "50|doi_________::";
    private static final Logger log = LoggerFactory.getLogger(PrepareDatasetTest.class);

    @BeforeAll
    public static void beforeAll() throws IOException {
        workingDir = Files.createTempDirectory(PrepareDatasetTest.class.getSimpleName());

        log.info("Using work dir {}", workingDir);

        SparkConf conf = new SparkConf();
        conf.setAppName(PrepareDatasetTest.class.getSimpleName());

        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.sql.warehouse.dir", workingDir.toString());
        conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

        spark = SparkSession
                .builder()
                .appName(PrepareDatasetTest.class.getSimpleName())
                .config(conf)
                .getOrCreate();
    }

    @AfterAll
    public static void afterAll() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
        spark.stop();
    }

    @Test
    void testMatch() throws Exception {

        String oalexPath = getClass()
                .getResource("/eu/dnetlib/dhp/actionmanager/affro/dataset/oalex")
                .getPath();

        String oairePath = getClass()
                .getResource("/eu/dnetlib/dhp/actionmanager/affro/dataset/graph")
                .getPath();

        String publishersPath = getClass()
                .getResource("/eu/dnetlib/dhp/actionmanager/affro/dataset/publisher")
                .getPath();

        String iisPath = getClass()
                .getResource("/eu/dnetlib/dhp/actionmanager/affro/dataset/iis")
                .getPath();

        String oldMatches = getClass()
                .getResource("/eu/dnetlib/dhp/actionmanager/affro/oldMatches/oldMatch")
                .getPath();

        String outputPath = workingDir.toString();


        PrepareDataset
                .main(
                        new String[]{
                                "-isSparkSessionManaged", Boolean.FALSE.toString(),
                                "-oalexPath", oalexPath,
                                "-oairePath", oairePath,
                                "-publishersPath", publishersPath,
                                "-iisPath", iisPath,
                                "-oldMatches", oldMatches,
                                "-outputPath", outputPath,
                                "-applyOnAll", Boolean.TRUE.toString(),
                                "-workingDir", workingDir.toString()
                        });

        Dataset<Row> dataset = spark.read().schema(AFFILIATION_STRING_SCHEMA).json(outputPath + "/toMatch");
        Assertions.assertTrue(dataset.count() == dataset.distinct().count());
    }
}
