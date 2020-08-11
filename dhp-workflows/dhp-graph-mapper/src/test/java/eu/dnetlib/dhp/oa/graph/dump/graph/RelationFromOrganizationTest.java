
package eu.dnetlib.dhp.oa.graph.dump.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Relation;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class RelationFromOrganizationTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static SparkSession spark;

    private static Path workingDir;

    private static final Logger log = LoggerFactory
            .getLogger(RelationFromOrganizationTest.class);

    private static HashMap<String, String> map = new HashMap<>();

    @BeforeAll
    public static void beforeAll() throws IOException {
        workingDir = Files
                .createTempDirectory(RelationFromOrganizationTest.class.getSimpleName());
        log.info("using work dir {}", workingDir);

        SparkConf conf = new SparkConf();
        conf.setAppName(RelationFromOrganizationTest.class.getSimpleName());

        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.sql.warehouse.dir", workingDir.toString());
        conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

        spark = SparkSession
                .builder()
                .appName(RelationFromOrganizationTest.class.getSimpleName())
                .config(conf)
                .getOrCreate();
    }

    @AfterAll
    public static void afterAll() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
        spark.stop();
    }

    @Test
    public void test1() throws Exception {

        final String sourcePath = getClass()
                .getResource("/eu/dnetlib/dhp/oa/graph/dump/relation")
                .getPath();

        SparkOrganizationRelation.main(new String[] {
                "-isSparkSessionManaged", Boolean.FALSE.toString(),
                "-outputPath", workingDir.toString() + "/relation",
                "-sourcePath", sourcePath
        });

//		dumpCommunityProducts.exec(MOCK_IS_LOOK_UP_URL,Boolean.FALSE, workingDir.toString()+"/dataset",sourcePath,"eu.dnetlib.dhp.schema.oaf.Dataset","eu.dnetlib.dhp.schema.dump.oaf.Dataset");

        final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        JavaRDD<Relation> tmp = sc
                .textFile(workingDir.toString() + "/relation")
                .map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

        org.apache.spark.sql.Dataset<Relation> verificationDataset = spark
                .createDataset(tmp.rdd(), Encoders.bean(Relation.class));

        verificationDataset.createOrReplaceTempView("table");

    }
