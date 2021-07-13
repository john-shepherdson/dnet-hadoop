package eu.dnetlib.dhp.oa.graph.dump.complete;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Relation;
import net.sf.saxon.expr.instruct.ForEach;

public class SelectRelationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static SparkSession spark;

    private static Path workingDir;

    private static final Logger log = LoggerFactory
            .getLogger(SelectRelationTest.class);

    private static HashMap<String, String> map = new HashMap<>();

    @BeforeAll
    public static void beforeAll() throws IOException {
        workingDir = Files
                .createTempDirectory(SelectRelationTest.class.getSimpleName());
        log.info("using work dir {}", workingDir);

        SparkConf conf = new SparkConf();
        conf.setAppName(SelectRelationTest.class.getSimpleName());

        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.sql.warehouse.dir", workingDir.toString());
        conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

        spark = SparkSession
                .builder()
                .appName(SelectRelationTest.class.getSimpleName())
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
                .getResource("/eu/dnetlib/dhp/oa/graph/dump/selectrelations")
                .getPath();

        SparkSelectValidRelationsJob.main(new String[] {
                "-isSparkSessionManaged", Boolean.FALSE.toString(),
                "-outputPath", workingDir.toString() + "/relation",
                "-sourcePath", sourcePath
        });

//		dumpCommunityProducts.exec(MOCK_IS_LOOK_UP_URL,Boolean.FALSE, workingDir.toString()+"/dataset",sourcePath,"eu.dnetlib.dhp.schema.oaf.Dataset","eu.dnetlib.dhp.schema.dump.oaf.Dataset");

        final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        JavaRDD<eu.dnetlib.dhp.schema.oaf.Relation> tmp = sc
                .textFile(workingDir.toString() + "/relation")
                .map(item -> OBJECT_MAPPER.readValue(item, eu.dnetlib.dhp.schema.oaf.Relation.class));

        org.apache.spark.sql.Dataset<Relation> verificationDataset = spark
                .createDataset(tmp.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.oaf.Relation.class));

        Assertions.assertTrue(verificationDataset.count() == 7);

    }

}