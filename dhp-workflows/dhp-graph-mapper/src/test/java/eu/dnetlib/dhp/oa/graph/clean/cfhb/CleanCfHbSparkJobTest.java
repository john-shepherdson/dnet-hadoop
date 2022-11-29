package eu.dnetlib.dhp.oa.graph.clean.cfhb;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Publication;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CleanCfHbSparkJobTest {

    private static final Logger log = LoggerFactory.getLogger(CleanCfHbSparkJobTest.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static SparkSession spark;

    private static Path testBaseTmpPath;

    private static String resolvedPath;

    private static String graphInputPath;

    private static String graphOutputPath;

    private static String dsMasterDuplicatePath;

    @BeforeAll
    public static void beforeAll() throws IOException, URISyntaxException {

        testBaseTmpPath = Files.createTempDirectory(CleanCfHbSparkJobTest.class.getSimpleName());
        log.info("using test base path {}", testBaseTmpPath);

        final File entitiesSources = Paths
                .get(CleanCfHbSparkJobTest.class.getResource("/eu/dnetlib/dhp/oa/graph/clean/cfhb/entities").toURI())
                .toFile();

        FileUtils
                .copyDirectory(
                        entitiesSources,
                        testBaseTmpPath.resolve("input").resolve("entities").toFile());

        FileUtils
                .copyFileToDirectory(
                        Paths
                                .get(CleanCfHbSparkJobTest.class.getResource("/eu/dnetlib/dhp/oa/graph/clean/cfhb/masterduplicate.json").toURI())
                                .toFile(),
                        testBaseTmpPath.resolve("workingDir").resolve("masterduplicate").toFile());


        graphInputPath = testBaseTmpPath.resolve("input").resolve("entities").toString();
        resolvedPath = testBaseTmpPath.resolve("workingDir").resolve("cfHbResolved").toString();
        graphOutputPath = testBaseTmpPath.resolve("workingDir").resolve("cfHbPatched").toString();
        dsMasterDuplicatePath = testBaseTmpPath.resolve("workingDir").resolve("masterduplicate").toString();

        SparkConf conf = new SparkConf();
        conf.setAppName(CleanCfHbSparkJobTest.class.getSimpleName());

        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("spark.ui.enabled", "false");

        spark = SparkSession
                .builder()
                .appName(CleanCfHbSparkJobTest.class.getSimpleName())
                .config(conf)
                .getOrCreate();
    }

    @AfterAll
    public static void afterAll() throws IOException {
        FileUtils.deleteDirectory(testBaseTmpPath.toFile());
        spark.stop();
    }

    @Test
    void testCleanCfHbSparkJob() throws Exception {
        final String outputPath = graphOutputPath + "/dataset";
        CleanCfHbSparkJob
                .main(
                        new String[] {
                                "--isSparkSessionManaged", Boolean.FALSE.toString(),
                                "--inputPath", graphInputPath + "/dataset",
                                "--outputPath", outputPath,
                                "--resolvedPath", resolvedPath + "/dataset",
                                "--graphTableClassName", Dataset.class.getCanonicalName(),
                                "--datasourceMasterDuplicate", dsMasterDuplicatePath
                        });

        //final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Assertions.assertTrue(Files.exists(Paths.get(graphOutputPath, "dataset")));

        final org.apache.spark.sql.Dataset<Dataset> d = spark
                .read()
                .textFile(outputPath)
                .map(as(Dataset.class), Encoders.bean(Dataset.class));
        Assertions.assertEquals(3, d.count());
        
    }

    private static <R> MapFunction<String, R> as(Class<R> clazz) {
        return s -> OBJECT_MAPPER.readValue(s, clazz);
    }
}
