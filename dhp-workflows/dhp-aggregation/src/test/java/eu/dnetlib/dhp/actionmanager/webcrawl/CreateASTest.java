package eu.dnetlib.dhp.actionmanager.webcrawl;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.PidCleaner;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;

/**
 * @author miriam.baglioni
 * @Date 22/04/24
 */
public class CreateASTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static SparkSession spark;

    private static Path workingDir;
    private static final Logger log = LoggerFactory
            .getLogger(CreateASTest.class);

    @BeforeAll
    public static void beforeAll() throws IOException {
        workingDir = Files
                .createTempDirectory(CreateASTest.class.getSimpleName());
        log.info("using work dir {}", workingDir);

        SparkConf conf = new SparkConf();
        conf.setAppName(CreateASTest.class.getSimpleName());

        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.sql.warehouse.dir", workingDir.toString());
        conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

        spark = SparkSession
                .builder()
                .appName(CreateASTest.class.getSimpleName())
                .config(conf)
                .getOrCreate();
    }

    @AfterAll
    public static void afterAll() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
        spark.stop();
    }
    @Test
    void testNumberofRelations() throws Exception {

        String inputPath = getClass()
                .getResource(
                        "/eu/dnetlib/dhp/actionmanager/webcrawl/")
                .getPath();

        CreateActionSetFromWebEntries
                .main(
                        new String[] {
                                "-isSparkSessionManaged",
                                Boolean.FALSE.toString(),
                                "-sourcePath",
                                inputPath,
                                "-outputPath",
                                workingDir.toString() + "/actionSet1"
                        });

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Relation> tmp = sc
                .sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
                .map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
                .map(aa -> ((Relation) aa.getPayload()));

        Assertions.assertEquals(64, tmp.count());

    }
    @Test
    void testRelations() throws Exception {

//		 , "doi":"https://doi.org/10.1126/science.1188021", "pmid":"https://pubmed.ncbi.nlm.nih.gov/20448178", https://www.ncbi.nlm.nih.gov/pmc/articles/5100745

        String inputPath = getClass()
                .getResource(
                        "/eu/dnetlib/dhp/actionmanager/webcrawl/")
                .getPath();

        CreateActionSetFromWebEntries
                .main(
                        new String[] {
                                "-isSparkSessionManaged",
                                Boolean.FALSE.toString(),
                                "-sourcePath",
                                inputPath,
                                "-outputPath",
                                workingDir.toString() + "/actionSet1"
                        });

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Relation> tmp = sc
                .sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
                .map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
                .map(aa -> ((Relation) aa.getPayload()));

        tmp.foreach(r -> System.out.println(new ObjectMapper().writeValueAsString(r)));

        Assertions
                .assertEquals(
                        1, tmp
                                .filter(
                                        r -> r
                                                .getSource()
                                                .equals(
                                                        "50|doi_________::" + IdentifierFactory
                                                                .md5(
                                                                        PidCleaner
                                                                                .normalizePidValue(PidType.doi.toString(), "10.1098/rstl.1684.0023"))))
                                .count());

        Assertions
                .assertEquals(
                        1, tmp
                                .filter(
                                        r -> r
                                                .getTarget()
                                                .equals(
                                                        "50|doi_________::" + IdentifierFactory
                                                                .md5(
                                                                        PidCleaner
                                                                                .normalizePidValue(PidType.doi.toString(), "10.1098/rstl.1684.0023"))))
                                .count());

        Assertions
                .assertEquals(
                        1, tmp
                                .filter(
                                        r -> r
                                                .getSource()
                                                .equals(
                                                        "20|ror_________::" + IdentifierFactory
                                                                .md5(
                                                                        PidCleaner
                                                                                .normalizePidValue("ROR", "https://ror.org/03argrj65"))))
                                .count());

        Assertions
                .assertEquals(
                        1, tmp
                                .filter(
                                        r -> r
                                                .getTarget()
                                                .equals(
                                                        "20|ror_________::" + IdentifierFactory
                                                                .md5(
                                                                        PidCleaner
                                                                                .normalizePidValue("ROR", "https://ror.org/03argrj65"))))
                                .count());

        Assertions
                .assertEquals(
                        5, tmp
                                .filter(
                                        r -> r
                                                .getSource()
                                                .equals(
                                                        "20|ror_________::" + IdentifierFactory
                                                                .md5(
                                                                        PidCleaner
                                                                                .normalizePidValue("ROR", "https://ror.org/03265fv13"))))
                                .count());

        Assertions
                .assertEquals(
                        5, tmp
                                .filter(
                                        r -> r
                                                .getTarget()
                                                .equals(
                                                        "20|ror_________::" + IdentifierFactory
                                                                .md5(
                                                                        PidCleaner
                                                                                .normalizePidValue("ROR", "https://ror.org/03265fv13"))))
                                .count());

        Assertions
                .assertEquals(
                        2, tmp
                                .filter(
                                        r -> r
                                                .getTarget()
                                                .equals(
                                                        "20|ror_________::" + IdentifierFactory
                                                                .md5(
                                                                        PidCleaner
                                                                                .normalizePidValue(PidType.doi.toString(), "https://ror.org/03265fv13")))
                                                && r.getSource().startsWith("50|doi"))
                                .count());

        Assertions
                .assertEquals(
                        2, tmp
                                .filter(
                                        r -> r
                                                .getTarget()
                                                .equals(
                                                        "20|ror_________::" + IdentifierFactory
                                                                .md5(
                                                                        PidCleaner
                                                                                .normalizePidValue(PidType.doi.toString(), "https://ror.org/03265fv13")))
                                                && r.getSource().startsWith("50|pmid"))
                                .count());

        Assertions
                .assertEquals(
                        1, tmp
                                .filter(
                                        r -> r
                                                .getTarget()
                                                .equals(
                                                        "20|ror_________::" + IdentifierFactory
                                                                .md5(
                                                                        PidCleaner
                                                                                .normalizePidValue(PidType.doi.toString(), "https://ror.org/03265fv13")))
                                                && r.getSource().startsWith("50|pmc"))
                                .count());
    }

    @Test
    void testRelationsCollectedFrom() throws Exception {

        String inputPath = getClass()
                .getResource(
                        "/eu/dnetlib/dhp/actionmanager/webcrawl")
                .getPath();

        CreateActionSetFromWebEntries
                .main(
                        new String[] {
                                "-isSparkSessionManaged",
                                Boolean.FALSE.toString(),
                                "-sourcePath",
                                inputPath,
                                "-outputPath",
                                workingDir.toString() + "/actionSet1"
                        });

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Relation> tmp = sc
                .sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
                .map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
                .map(aa -> ((Relation) aa.getPayload()));

        tmp.foreach(r -> {
            assertEquals("Web Crawl", r.getCollectedfrom().get(0).getValue());
            assertEquals("10|openaire____::fb98a192f6a055ba495ef414c330834b", r.getCollectedfrom().get(0).getKey());
        });

    }



}
