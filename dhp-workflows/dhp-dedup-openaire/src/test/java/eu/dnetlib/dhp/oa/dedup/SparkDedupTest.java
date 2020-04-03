package eu.dnetlib.dhp.oa.dedup;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import net.bytebuddy.utility.RandomString;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.dom4j.DocumentException;
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

    SparkSession spark;

    static String testGraphBasePath;
    static String testOutputBasePath;
    String testActionSetId = "test-orchestrator";

    @BeforeEach
    public void setUp() throws IOException, ISLookUpException, DocumentException, URISyntaxException {

        testGraphBasePath = Paths.get(getClass().getResource("/eu/dnetlib/dhp/dedup/entities").toURI()).toFile().getAbsolutePath();
        testOutputBasePath = "/tmp/" + RandomString.make(5).toLowerCase();
        spark = SparkSession
                .builder()
                .appName(SparkCreateSimRels.class.getSimpleName())
                .master("local[*]")
                .config(new SparkConf())
                .getOrCreate();

        when(isLookUpService.getResourceProfileByQuery(Mockito.contains(testActionSetId)))
                .thenReturn(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/dedup/profiles/mock_orchestrator.xml")));

        when(isLookUpService.getResourceProfileByQuery(Mockito.contains("organization")))
                .thenReturn(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json")));

        when(isLookUpService.getResourceProfileByQuery(Mockito.contains("publication")))
                .thenReturn(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/pub.curr.conf.json")));
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

        //test number of simrels
        long orgs_simrel = spark.read().load(testOutputBasePath + "/" + testActionSetId + "/organization_simrel").count();
        long pubs_simrel = spark.read().load(testOutputBasePath + "/" + testActionSetId + "/publication_simrel").count();

        assertEquals(918, orgs_simrel);
        assertEquals(0, pubs_simrel);

    }

//    @Disabled("must be parametrized to run locally")
//    public void createCCTest() throws Exception {
//
//        SparkCreateConnectedComponent.main(new String[]{
//                "-mt", "local[*]",
//                "-s", "/Users/miconis/dumps",
//                "-e", entity,
//                "-c", ArgumentApplicationParser.compressArgument(configuration),
//                "-t", "/tmp/dedup",
//        });
//    }
//
//    @Disabled("must be parametrized to run locally")
//    public void dedupRecordTest() throws Exception {
//        SparkCreateDedupRecord.main(new String[]{
//                "-mt", "local[*]",
//                "-s", "/Users/miconis/dumps",
//                "-e", entity,
//                "-c", ArgumentApplicationParser.compressArgument(configuration),
//                "-d", "/tmp/dedup",
//        });
//    }
//
//    @Disabled("must be parametrized to run locally")
//    public void printConfiguration() throws Exception {
//        System.out.println(ArgumentApplicationParser.compressArgument(configuration));
//    }
//
//    @Disabled("must be parametrized to run locally")
//    public void testHashCode() {
//        final String s1 = "20|grid________::6031f94bef015a37783268ec1e75f17f";
//        final String s2 = "20|nsf_________::b12be9edf414df8ee66b4c52a2d8da46";
//
//        final HashFunction hashFunction = Hashing.murmur3_128();
//
//        System.out.println(s1.hashCode());
//        System.out.println(hashFunction.hashString(s1).asLong());
//        System.out.println(s2.hashCode());
//        System.out.println(hashFunction.hashString(s2).asLong());
//    }
//
//    public List<DedupConfig> prepareConfigurations() throws IOException {
//
//        return Lists.newArrayList(
//                DedupConfig.load(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json"))),
//                DedupConfig.load(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json")))
//        );
//    }

    @AfterAll
    public static void cleanUp() throws IOException {
        FileUtils.deleteDirectory(new File(testOutputBasePath));
    }
}
