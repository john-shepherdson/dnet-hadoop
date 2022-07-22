package eu.dnetlib.dhp.oa.graph.clean;
/**
 * @author miriam.baglioni
 * @Date 20/07/22
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.oa.graph.clean.country.CleanCountrySparkJob;
import eu.dnetlib.dhp.oa.graph.dump.DumpJobTest;
import eu.dnetlib.dhp.schema.oaf.Publication;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
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


public class CleanCountryTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static SparkSession spark;

    private static Path workingDir;

    private static final Logger log = LoggerFactory.getLogger(CleanContextTest.class);

    @BeforeAll
    public static void beforeAll() throws IOException {
        workingDir = Files.createTempDirectory(DumpJobTest.class.getSimpleName());
        log.info("using work dir {}", workingDir);

        SparkConf conf = new SparkConf();
        conf.setAppName(DumpJobTest.class.getSimpleName());

        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.sql.warehouse.dir", workingDir.toString());
        conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

        spark = SparkSession
                .builder()
                .appName(DumpJobTest.class.getSimpleName())
                .config(conf)
                .getOrCreate();
    }

    @AfterAll
    public static void afterAll() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
        spark.stop();
    }

    @Test
    public void testResultClean() throws Exception {
        final String sourcePath = getClass()
                .getResource("/eu/dnetlib/dhp/oa/graph/clean/publication_clean_country.json")
                .getPath();
        final String prefix = "gcube ";

        spark
                .read()
                .textFile(sourcePath)
                .map(
                        (MapFunction<String, Publication>) r -> OBJECT_MAPPER.readValue(r, Publication.class),
                        Encoders.bean(Publication.class))
                .write()
                .json(workingDir.toString() + "/publication");

        CleanCountrySparkJob.main(new String[] {
                "--isSparkSessionManaged", Boolean.FALSE.toString(),
                "--inputPath", workingDir.toString() + "/publication",
                "-graphTableClassName", Publication.class.getCanonicalName(),
                "-workingPath", workingDir.toString() + "/working",
                "-country", "NL",
                "-verifyParam", "10.17632",
                "-collectedfrom", "NARCIS"
        });

        final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<Publication> tmp = sc
                .textFile(workingDir.toString() + "/publication")
                .map(item -> OBJECT_MAPPER.readValue(item, Publication.class));

        Assertions.assertEquals(7, tmp.count());

        // original result with NL country  and doi starting with Mendely prefix, but not collectedfrom NARCIS
        Assertions
                .assertEquals(
                        1,
                        tmp
                                .filter(p -> p.getId().equals("50|DansKnawCris::0224aae28af558f21768dbc6439c7a95"))
                                .collect()
                                .get(0)
                                .getCountry()
                                .size());

        // original result with NL country and pid not starting with Mendely prefix
        Assertions
                .assertEquals(
                        1,
                        tmp
                                .filter(p -> p.getId().equals("50|DansKnawCris::20c414a3b1c742d5dd3851f1b67df2d9"))
                                .collect()
                                .get(0)
                                .getCountry()
                                .size());

        // original result with NL country  and doi starting with Mendely prefix and collectedfrom NARCIS
        Assertions
                .assertEquals(
                        0,
                        tmp
                                .filter(p -> p.getId().equals("50|DansKnawCris::3c81248c335f0aa07e06817ece6fa6af"))
                                .collect()
                                .get(0)
                                .getCountry()
                                .size());
    }

}
