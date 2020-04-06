package eu.dnetlib.dhp.oa.dedup;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SparkDedupTest {

    @Mock
    ISLookUpService isLookUpService;

    private SparkSession spark;

    private static String testGraphBasePath;
    private final static String testOutputBasePath = "/tmp/test_dedup_workflow";
    private final static String testActionSetId = "test-orchestrator";
    private final static String testDedupGraphBasePath = "/tmp/test_dedup_workflow/dedup_graph";

    @BeforeAll
    private static void cleanUp() throws IOException, URISyntaxException {

        testGraphBasePath = Paths.get(SparkDedupTest.class.getResource("/eu/dnetlib/dhp/dedup/entities").toURI()).toFile().getAbsolutePath();

        FileUtils.deleteDirectory(new File(testOutputBasePath));
    }

    @BeforeEach
    private void setUp() throws IOException, ISLookUpException {

        spark = SparkSession
                .builder()
                .appName(SparkCreateSimRels.class.getSimpleName())
                .master("local[*]")
                .config(new SparkConf())
                .getOrCreate();

        when(isLookUpService.getResourceProfileByQuery(Mockito.contains(testActionSetId)))
                .thenReturn(IOUtils.toString(SparkDedupTest.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/profiles/mock_orchestrator.xml")));

        when(isLookUpService.getResourceProfileByQuery(Mockito.contains("organization")))
                .thenReturn(IOUtils.toString(SparkDedupTest.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json")));

        when(isLookUpService.getResourceProfileByQuery(Mockito.contains("publication")))
                .thenReturn(IOUtils.toString(SparkDedupTest.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/pub.curr.conf.json")));

    }

    @Test
    @Order(1)
    public void createSimRelsTest() throws Exception {

        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createSimRels_parameters.json")));
        parser.parseArgument(new String[]{
                "-mt", "local[*]",
                "-i", testGraphBasePath,
                "-asi", testActionSetId,
                "-la", "lookupurl",
                "-w", testOutputBasePath});

        new SparkCreateSimRels(parser, spark).run(isLookUpService);

        long orgs_simrel = spark.read().load(testOutputBasePath + "/" + testActionSetId + "/organization_simrel").count();
        long pubs_simrel = spark.read().load(testOutputBasePath + "/" + testActionSetId + "/publication_simrel").count();

        System.out.println("pubs_simrel = " + pubs_simrel);
        System.out.println("orgs_simrel = " + orgs_simrel);
//        assertEquals(918, orgs_simrel);
//        assertEquals(0, pubs_simrel);

    }

    @Test
    @Order(2)
    public void createCCTest() throws Exception {

        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateConnectedComponent.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json")));
        parser.parseArgument(new String[]{
                "-mt", "local[*]",
                "-i", testGraphBasePath,
                "-asi", testActionSetId,
                "-la", "lookupurl",
                "-w", testOutputBasePath});

        new SparkCreateConnectedComponent(parser, spark).run(isLookUpService);

        long orgs_mergerel = spark.read().load(testOutputBasePath + "/" + testActionSetId + "/organization_mergerel").count();
        long pubs_mergerel = spark.read().load(testOutputBasePath + "/" + testActionSetId + "/publication_mergerel").count();

        System.out.println("pubs_mergerel = " + pubs_mergerel);
        System.out.println("orgs_mergerel = " + orgs_mergerel);
//        assertEquals(458, orgs_mergerel);
//        assertEquals(0, pubs_mergerel);

    }

    @Test
    @Order(3)
    public void dedupRecordTest() throws Exception {

        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateDedupRecord.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createDedupRecord_parameters.json")));
        parser.parseArgument(new String[]{
                "-mt", "local[*]",
                "-i", testGraphBasePath,
                "-asi", testActionSetId,
                "-la", "lookupurl",
                "-w", testOutputBasePath});

        new SparkCreateDedupRecord(parser, spark).run(isLookUpService);

        long orgs_deduprecord = spark.read().load(testOutputBasePath + "/" + testActionSetId + "/organization_deduprecord").count();
        long pubs_deduprecord = spark.read().load(testOutputBasePath + "/" + testActionSetId + "/publication_deduprecord").count();

        System.out.println("pubs_deduprecord = " + pubs_deduprecord);
        System.out.println("pubs_deduprecord = " + orgs_deduprecord);
//        assertEquals(458, orgs_deduprecord);
//        assertEquals(0, pubs_deduprecord);
    }

    @Test
    @Order(4)
    public void updateEntityTest() throws Exception {

        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkUpdateEntity.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/updateEntity_parameters.json")));
        parser.parseArgument(new String[]{
                "-mt", "local[*]",
                "-i", testGraphBasePath,
                "-w", testOutputBasePath,
                "-o", testDedupGraphBasePath
        });

        new SparkUpdateEntity(parser, spark).run(isLookUpService);

        long organizations = spark.read().load(testDedupGraphBasePath + "/organization").count();
        long publications = spark.read().load(testDedupGraphBasePath + "/publication").count();

        System.out.println("publications = " + publications);
        System.out.println("organizations = " + organizations);

    }

    @Test
    @Order(5)
    public void propagateRelationTest() throws Exception {

        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkPropagateRelation.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/propagateRelation_parameters.json")));
        parser.parseArgument(new String[]{
                "-mt", "local[*]",
                "-i", testGraphBasePath,
                "-w", testOutputBasePath,
                "-o", testDedupGraphBasePath
        });

        new SparkPropagateRelation(parser, spark).run(isLookUpService);

        long relations = spark.read().load(testDedupGraphBasePath + "/relation").count();

        System.out.println("relations = " + relations);

    }

    @Disabled("must be parametrized to run locally")
    public void testHashCode() {
        final String s1 = "20|grid________::6031f94bef015a37783268ec1e75f17f";
        final String s2 = "20|nsf_________::b12be9edf414df8ee66b4c52a2d8da46";

        final HashFunction hashFunction = Hashing.murmur3_128();

        System.out.println(s1.hashCode());
        System.out.println(hashFunction.hashString(s1).asLong());
        System.out.println(s2.hashCode());
        System.out.println(hashFunction.hashString(s2).asLong());
    }
}
