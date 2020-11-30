package eu.dnetlib.dhp.actionmanager.bipfinder;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class SparkAtomicActionScoreJobTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static SparkSession spark;

    private static Path workingDir;
    private static final Logger log = LoggerFactory
            .getLogger(SparkAtomicActionScoreJobTest.class);

    @BeforeAll
    public static void beforeAll() throws IOException {
        workingDir = Files
                .createTempDirectory(SparkAtomicActionScoreJobTest.class.getSimpleName());
        log.info("using work dir {}", workingDir);

        SparkConf conf = new SparkConf();
        conf.setAppName(SparkAtomicActionScoreJobTest.class.getSimpleName());

        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.sql.warehouse.dir", workingDir.toString());
        conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

        spark = SparkSession
                .builder()
                .appName(SparkAtomicActionScoreJobTest.class.getSimpleName())
                .config(conf)
                .getOrCreate();
    }

    @AfterAll
    public static void afterAll() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
        spark.stop();
    }

    @Test
    public <I extends Result> void numberDistinctProjectTest() throws Exception {
        String bipScoresPath =  getClass().getResource("/eu/dnetlib/dhp/actionmanager/bipfinder/bip_scores.json").getPath();
        String inputPath = getClass()
                .getResource(
                        "/eu/dnetlib/dhp/actionmanager/bipfinder/publication.json")
                .getPath();

        SparkAtomicActionScoreJob.main(
                        new String[] {
                                "-isSparkSessionManaged",
                                Boolean.FALSE.toString(),
                                "-inputPath",
                                inputPath,
                                "-bipScorePath",
                               bipScoresPath,
                                "-resultTableName",
                                "eu.dnetlib.dhp.schema.oaf.Publication",
                                "-outputPath",
                                workingDir.toString() + "/actionSet"
                        });


        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Publication> tmp = sc
                .sequenceFile(workingDir.toString() + "/actionSet", Text.class, Text.class)
                .map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
                .map(aa -> ((Publication) aa.getPayload()));

        Assertions.assertTrue(tmp.count() == 1);

//        Dataset<Publication> verificationDataset = spark.createDataset(tmp.rdd(), Encoders.bean(Publication.class));
//        verificationDataset.createOrReplaceTempView("project");

//        Dataset<Row> execverification = spark
//                .sql(
//                        "SELECT id, class classification, h2020topiccode, h2020topicdescription FROM project LATERAL VIEW EXPLODE(h2020classification) c as class ");
//
//        Assertions
//                .assertEquals(
//                        "H2020-EU.3.4.7.",
//                        execverification
//                                .filter("id = '40|corda__h2020::2c7298913008865ba784e5c1350a0aa5'")
//                                .select("classification.h2020Programme.code")
//                                .collectAsList()
//                                .get(0)
//                                .getString(0));




    }

    private static List<Measure> getMeasure(BipScore value) {
        return value.getScoreList()
                .stream()
                .map(score -> {
                    Measure m = new Measure();
                    m.setId(score.getId());
                    m.setUnit(score.getUnit().stream()
                            .map(unit -> {
                                eu.dnetlib.dhp.schema.oaf.KeyValue kv = new KeyValue();
                                kv.setValue(unit.getValue());
                                kv.setKey(unit.getKey());
                                kv.setDataInfo(getDataInfo());
                                return kv;
                            }).collect(Collectors.toList()));
                    return m;
                }).collect(Collectors.toList());
    }


    private static DataInfo getDataInfo() {
        DataInfo di = new DataInfo();
        di.setInferred(false);
        di.setInvisible(false);
        di.setDeletedbyinference(false);
        di.setTrust("");
        Qualifier qualifier = new Qualifier();
        qualifier.setClassid("sysimport:actionset");
        qualifier.setClassname("Harvested");
        qualifier.setSchemename("dnet:provenanceActions");
        qualifier.setSchemeid("dnet:provenanceActions");
        di.setProvenanceaction(qualifier);
        return di;
    }
}
