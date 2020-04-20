package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import static java.nio.file.Files.createTempDirectory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SparkDedupTest implements Serializable {

    @Mock(serializable = true)
    ISLookUpService isLookUpService;

    private static SparkSession spark;
    private static JavaSparkContext jsc;

    private static String testGraphBasePath;
    private static String testOutputBasePath;
    private static String testDedupGraphBasePath;
    private final static String testActionSetId = "test-orchestrator";

    @BeforeAll
    private static void cleanUp() throws IOException, URISyntaxException {

        testGraphBasePath = Paths.get(SparkDedupTest.class.getResource("/eu/dnetlib/dhp/dedup/entities").toURI()).toFile().getAbsolutePath();

        testOutputBasePath = createTempDirectory(SparkDedupTest.class.getSimpleName() + "-").toAbsolutePath().toString();
        testDedupGraphBasePath = createTempDirectory(SparkDedupTest.class.getSimpleName() + "-").toAbsolutePath().toString();

        FileUtils.deleteDirectory(new File(testOutputBasePath));
        FileUtils.deleteDirectory(new File(testDedupGraphBasePath));

        spark = SparkSession
                .builder()
                .appName(SparkCreateSimRels.class.getSimpleName())
                .master("local[*]")
                .config(new SparkConf())
                .getOrCreate();

        jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

    }

    @BeforeEach
    private void setUp() throws IOException, ISLookUpException {

        lenient().when(isLookUpService.getResourceProfileByQuery(Mockito.contains(testActionSetId)))
                .thenReturn(IOUtils.toString(SparkDedupTest.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/profiles/mock_orchestrator.xml")));

        lenient().when(isLookUpService.getResourceProfileByQuery(Mockito.contains("organization")))
                .thenReturn(IOUtils.toString(SparkDedupTest.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json")));

        lenient().when(isLookUpService.getResourceProfileByQuery(Mockito.contains("publication")))
                .thenReturn(IOUtils.toString(SparkDedupTest.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/pub.curr.conf.json")));

        lenient().when(isLookUpService.getResourceProfileByQuery(Mockito.contains("software")))
                .thenReturn(IOUtils.toString(SparkDedupTest.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/sw.curr.conf.json")));

    }

    @Test
    @Order(1)
    public void createSimRelsTest() throws Exception {

        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createSimRels_parameters.json")));
        parser.parseArgument(new String[]{
                "-i", testGraphBasePath,
                "-asi", testActionSetId,
                "-la", "lookupurl",
                "-w", testOutputBasePath});

        new SparkCreateSimRels(parser, spark).run(isLookUpService);

        long orgs_simrel = spark.read().load(testOutputBasePath + "/" + testActionSetId + "/organization_simrel").count();
        long pubs_simrel = spark.read().load(testOutputBasePath + "/" + testActionSetId + "/publication_simrel").count();
        long sw_simrel = spark.read().load(testOutputBasePath + "/" + testActionSetId + "/software_simrel").count();

        assertEquals(3432, orgs_simrel);
        assertEquals(7260, pubs_simrel);
        assertEquals(344, sw_simrel);
    }

    @Test
    @Order(2)
    public void createMergeRelsTest() throws Exception {

        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateMergeRels.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json")));
        parser.parseArgument(new String[]{
                "-i", testGraphBasePath,
                "-asi", testActionSetId,
                "-la", "lookupurl",
                "-w", testOutputBasePath});

        new SparkCreateMergeRels(parser, spark).run(isLookUpService);

        long orgs_mergerel = spark.read().load(testOutputBasePath + "/" + testActionSetId + "/organization_mergerel").count();
        long pubs_mergerel = spark.read().load(testOutputBasePath + "/" + testActionSetId + "/publication_mergerel").count();
        long sw_mergerel = spark.read().load(testOutputBasePath + "/" + testActionSetId + "/software_mergerel").count();

        assertEquals(1276, orgs_mergerel);
        assertEquals(1460, pubs_mergerel);
        assertEquals(288, sw_mergerel);
    }

    @Test
    @Order(3)
    public void createDedupRecordTest() throws Exception {

        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateDedupRecord.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createDedupRecord_parameters.json")));
        parser.parseArgument(new String[]{
                "-i", testGraphBasePath,
                "-asi", testActionSetId,
                "-la", "lookupurl",
                "-w", testOutputBasePath});

        new SparkCreateDedupRecord(parser, spark).run(isLookUpService);

        long orgs_deduprecord = jsc.textFile(testOutputBasePath + "/" + testActionSetId + "/organization_deduprecord").count();
        long pubs_deduprecord = jsc.textFile(testOutputBasePath + "/" + testActionSetId + "/publication_deduprecord").count();
        long sw_deduprecord = jsc.textFile(testOutputBasePath + "/" + testActionSetId + "/software_deduprecord").count();

        assertEquals(82, orgs_deduprecord);
        assertEquals(66, pubs_deduprecord);
        assertEquals(51, sw_deduprecord);
    }

    @Test
    @Order(4)
    public void updateEntityTest() throws Exception {

        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkUpdateEntity.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/updateEntity_parameters.json")));
        parser.parseArgument(new String[]{
                "-i", testGraphBasePath,
                "-w", testOutputBasePath,
                "-o", testDedupGraphBasePath
        });

        new SparkUpdateEntity(parser, spark).run(isLookUpService);

        long organizations = jsc.textFile(testDedupGraphBasePath + "/organization").count();
        long publications = jsc.textFile(testDedupGraphBasePath + "/publication").count();
        long projects = jsc.textFile(testDedupGraphBasePath + "/project").count();
        long datasource = jsc.textFile(testDedupGraphBasePath + "/datasource").count();
        long softwares = jsc.textFile(testDedupGraphBasePath + "/software").count();

        long mergedOrgs = spark
                .read().load(testOutputBasePath + "/" + testActionSetId + "/organization_mergerel").as(Encoders.bean(Relation.class))
                .where("relClass=='merges'")
                .javaRDD()
                .map(Relation::getTarget)
                .distinct().count();

        long mergedPubs = spark
                .read().load(testOutputBasePath + "/" + testActionSetId + "/publication_mergerel").as(Encoders.bean(Relation.class))
                .where("relClass=='merges'")
                .javaRDD()
                .map(Relation::getTarget)
                .distinct().count();

        assertEquals(831, publications);
        assertEquals(835, organizations);
        assertEquals(100, projects);
        assertEquals(100, datasource);
        assertEquals(200, softwares);

        long deletedOrgs = jsc.textFile(testDedupGraphBasePath + "/organization")
                .filter(this::isDeletedByInference).count();
        long deletedPubs = jsc.textFile(testDedupGraphBasePath + "/publication")
                .filter(this::isDeletedByInference).count();

        assertEquals(mergedOrgs, deletedOrgs);
        assertEquals(mergedPubs, deletedPubs);
    }

    @Test
    @Order(5)
    public void propagateRelationTest() throws Exception {

        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkPropagateRelation.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/propagateRelation_parameters.json")));
        parser.parseArgument(new String[]{
                "-i", testGraphBasePath,
                "-w", testOutputBasePath,
                "-o", testDedupGraphBasePath
        });

        new SparkPropagateRelation(parser, spark).run(isLookUpService);

        long relations = jsc.textFile(testDedupGraphBasePath + "/relation").count();

        assertEquals(826, relations);

        //check deletedbyinference
        final Dataset<Relation> mergeRels = spark.read().load(DedupUtility.createMergeRelPath(testOutputBasePath, "*", "*")).as(Encoders.bean(Relation.class));
        final JavaPairRDD<String, String> mergedIds = mergeRels
                .where("relClass == 'merges'")
                .select(mergeRels.col("target"))
                .distinct()
                .toJavaRDD()
                .mapToPair((PairFunction<Row, String, String>) r -> new Tuple2<String, String>(r.getString(0), "d"));

        JavaRDD<String> toCheck = jsc.textFile(testDedupGraphBasePath + "/relation")
                .mapToPair(json -> new Tuple2<>(MapDocumentUtil.getJPathString("$.source", json), json))
                .join(mergedIds)
                .map(t -> t._2()._1())
                .mapToPair(json -> new Tuple2<>(MapDocumentUtil.getJPathString("$.target", json), json))
                .join(mergedIds)
                .map(t -> t._2()._1());

        long deletedbyinference = toCheck.filter(this::isDeletedByInference).count();
        long updated = toCheck.count();

        assertEquals(updated, deletedbyinference);
    }

    @AfterAll
    public static void finalCleanUp() throws IOException {
        FileUtils.deleteDirectory(new File(testOutputBasePath));
        FileUtils.deleteDirectory(new File(testDedupGraphBasePath));
    }

    public boolean isDeletedByInference(String s) {
        return  s.contains("\"deletedbyinference\":true");
    }
}