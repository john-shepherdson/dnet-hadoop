package eu.dnetlib.dhp.oa.graph.raw;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class PatchRelationApplicationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String ID_MAPPING_PATH = "map/id_mapping.json";

    private static SparkSession spark;

    private static Path workingDir;

    private static final Logger log = LoggerFactory.getLogger(PatchRelationApplicationTest.class);

    @BeforeAll
    public static void beforeAll() throws IOException {
        workingDir = Files
                .createTempDirectory(PatchRelationApplicationTest.class.getSimpleName());
        log.info("using work dir {}", workingDir);

        SparkConf conf = new SparkConf();
        conf.setAppName(PatchRelationApplicationTest.class.getSimpleName());

        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.sql.warehouse.dir", workingDir.toString());
        conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

        spark = SparkSession
                .builder()
                .appName(PatchRelationApplicationTest.class.getSimpleName())
                .config(conf)
                .getOrCreate();

        FileUtils.copyInputStreamToFile(
                PatchRelationApplicationTest.class.getResourceAsStream("id_mapping.json"),
                workingDir.resolve(ID_MAPPING_PATH).toFile()
        );

        FileUtils.copyInputStreamToFile(
                PatchRelationApplicationTest.class.getResourceAsStream("relations_to_patch.json"),
                workingDir.resolve("graphBasePath/relation/rels.json").toFile()
        );

    }

    @BeforeEach
    public void setUp() throws IOException {


    }

    @AfterAll
    public static void afterAll() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
        spark.stop();
    }

    @Test
    public void testPatchRelationApplication() throws Exception {

        PatchRelationsApplication.main(new String[] {
                "-isSparkSessionManaged", Boolean.FALSE.toString(),
                "-graphBasePath", workingDir.toString() + "/graphBasePath",
                "-workingDir", workingDir.toString() + "/workingDir",
                "-idMappingPath", workingDir.toString() + "/" + ID_MAPPING_PATH
        });

    }

}
