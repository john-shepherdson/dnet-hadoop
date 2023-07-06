package eu.dnetlib.dhp.actionmanager.bipaffiliations;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

import eu.dnetlib.dhp.schema.action.AtomicAction;

public class PrepareAffiliationRelationsTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static SparkSession spark;

    private static Path workingDir;
    private static final String ID_PREFIX = "50|doi_________::";
    private static final Logger log = LoggerFactory
            .getLogger(PrepareAffiliationRelationsTest.class);

    @BeforeAll
    public static void beforeAll() throws IOException {
        workingDir = Files.createTempDirectory(PrepareAffiliationRelationsTest.class.getSimpleName());

        log.info("Using work dir {}", workingDir);

        SparkConf conf = new SparkConf();
        conf.setAppName(PrepareAffiliationRelationsTest.class.getSimpleName());

        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.sql.warehouse.dir", workingDir.toString());
        conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

        spark = SparkSession
            .builder()
            .appName(PrepareAffiliationRelationsTest.class.getSimpleName())
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

        String affiliationRelationsPath = getClass()
            .getResource("/eu/dnetlib/dhp/actionmanager/bipaffiliations/doi_to_ror.json")
            .getPath();

        String outputPath = workingDir.toString() + "/actionSet";

        PrepareAffiliationRelations
            .main(
                new String[] {
                    "-isSparkSessionManaged", Boolean.FALSE.toString(),
                    "-inputPath", affiliationRelationsPath,
                    "-outputPath", outputPath
                });

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Relation> tmp = sc
            .sequenceFile(outputPath, Text.class, Text.class)
            .map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
            .map(aa -> ((Relation) aa.getPayload()));

        for (Relation r : tmp.collect()) {
            System.out.println(
                    r.getSource() + "\t" + r.getTarget() + "\t" + r.getRelType() + "\t" + r.getRelClass() + "\t" + r.getSubRelType() + "\t" + r.getValidationDate() + "\t" + r.getDataInfo().getTrust() + "\t" + r.getDataInfo().getInferred()
            );
        }
        // count the number of relations
        assertEquals(16, tmp.count());

        Dataset<Relation> dataset = spark.createDataset(tmp.rdd(), Encoders.bean(Relation.class));
        dataset.createOrReplaceTempView("result");

        Dataset<Row> execVerification = spark.sql("select r.relType, r.relClass, r.source, r.target, r.dataInfo.trust from result r");

        // verify that we have equal number of bi-directional relations
        Assertions.assertEquals(8, execVerification
            .filter(
                    "relClass='" + ModelConstants.HAS_AUTHOR_INSTITUTION +"'")
            .collectAsList()
            .size());

        Assertions.assertEquals(8, execVerification
            .filter(
                    "relClass='" + ModelConstants.IS_AUTHOR_INSTITUTION_OF +"'")
            .collectAsList()
            .size());

        // check confidence value of a specific relation
        String sourceDOI = "10.1105/tpc.8.3.343";

        final String sourceOpenaireId = ID_PREFIX + IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", sourceDOI));

        Assertions.assertEquals("0.7071067812", execVerification
            .filter(
                    "source='" + sourceOpenaireId +"'")
            .collectAsList().get(0).getString(4));

    }
}
