package eu.dnetlib.dhp.actionmanager.affro;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static eu.dnetlib.dhp.actionmanager.affro.Constants.AFFILIATION_STRING_SCHEMA;
import static eu.dnetlib.dhp.actionmanager.affro.Constants.RESULT_MATCHED_SCHEMA;
import static org.apache.spark.sql.functions.*;
public class RedistributeRelationsTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static SparkSession spark;

    private static Path workingDir;
    private static final String ID_PREFIX = "50|doi_________::";
    private static final Logger log = LoggerFactory.getLogger(RedistributeRelationsTest.class);

    @BeforeAll
    public static void beforeAll() throws IOException {
        workingDir = Files.createTempDirectory(RedistributeRelationsTest.class.getSimpleName());

        log.info("Using work dir {}", workingDir);

        SparkConf conf = new SparkConf();
        conf.setAppName(RedistributeRelationsTest.class.getSimpleName());

        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.sql.warehouse.dir", workingDir.toString());
        conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

        spark = SparkSession
                .builder()
                .appName(RedistributeRelationsTest.class.getSimpleName())
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


        String explodedResultPath = getClass()
                .getResource("/eu/dnetlib/dhp/actionmanager/affro/exploded")
                .getPath();

        String matchingsPath = getClass()
                .getResource("/eu/dnetlib/dhp/actionmanager/affro/matchings")
                .getPath();

        String outputPath = workingDir.toString() + "/actionSet";


        RedistributeRelations
                .main(
                        new String[]{
                                "-isSparkSessionManaged", Boolean.FALSE.toString(),
                                "-explodedResultPath", explodedResultPath,
                                "-matchingsPath", matchingsPath,

                                "-outputPath", outputPath,

                                "-workingDir", workingDir.toString()
                        });
    final String stringa = outputPath ;
        System.out.println(stringa);
        Dataset<Row> dataset = spark.read().schema(RESULT_MATCHED_SCHEMA).json(stringa);
        Assertions.assertEquals(32, dataset.count());
        Assertions.assertEquals(32, dataset.distinct().count());

        dataset.where("id = '50|doi_________::537bbda4fdfe87a42b239f27af5b14be'").
                withColumn("author", explode(col("authors"))).select("author").show(false);

        Assertions.assertEquals(6, dataset.where("id = '50|doi_________::537bbda4fdfe87a42b239f27af5b14be'")
                .selectExpr("size(authors) as authors_count")
                .first()
                .getInt(0));

        Assertions.assertEquals(3, dataset.where("id = '50|doi_________::537bbda4fdfe87a42b239f27af5b14be'")
                .selectExpr("size(organizations) as org_count")
                .first()
                .getInt(0));

        dataset.where("id = '50|doi_________::e57dc736f7f5d7c724faf105b85b9106'").show(false);

        Assertions.assertEquals(1, dataset.where("id = '50|doi_________::e57dc736f7f5d7c724faf105b85b9106'").selectExpr("size(authors) as authors_count")
                .first()
                .getInt(0));

        Row author = dataset.where("id = '50|doi_________::e57dc736f7f5d7c724faf105b85b9106'")
                .select(
                        explode(col("authors")).alias("author"))
                .first()
                .getAs("author");
        Assertions.assertEquals("Yatracos, Yannis G.", author.getAs("fullname"));
        Boolean isCorresponding = author.getAs("corresponding");
        Assertions.assertTrue(isCorresponding);


    }
}
