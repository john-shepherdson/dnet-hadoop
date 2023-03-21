package eu.dnetlib.dhp.oa.graph.clean;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.GraphCleaningFunctions;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
public class CleanGraphSparkJobTest {

    private static final Logger log = LoggerFactory.getLogger(CleanContextTest.class);

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Mock
    private ISLookUpService isLookUpService;

    private VocabularyGroup vocabularies;

    private CleaningRuleMap mapping;

    private static SparkSession spark;

    private static Path workingDir;

    private static Path testBaseTmpPath;

    private static String graphInputPath;

    private static String graphOutputPath;

    private static String dsMasterDuplicatePath;

    @BeforeAll
    public static void beforeAll() throws IOException, URISyntaxException {
        testBaseTmpPath = Files.createTempDirectory(CleanGraphSparkJobTest.class.getSimpleName());
        log.info("using test base path {}", testBaseTmpPath);

        File basePath = Paths
                .get(CleanGraphSparkJobTest.class.getResource("/eu/dnetlib/dhp/oa/graph/clean/graph").toURI())
                .toFile();


        List<File> paths = FileUtils
                .listFilesAndDirs(basePath, FalseFileFilter.FALSE, TrueFileFilter.TRUE)
                .stream()
                .filter(f -> !f.getAbsolutePath().endsWith("/graph"))
                .collect(Collectors.toList());

        for(File path : paths) {
            String type = StringUtils.substringAfterLast(path.getAbsolutePath(), "/");
            FileUtils
                    .copyDirectory(
                            path,
                            testBaseTmpPath.resolve("input").resolve("graph").resolve(type).toFile());
        }

        FileUtils
                .copyFileToDirectory(
                        Paths
                                .get(
                                        CleanGraphSparkJobTest.class
                                                .getResource("/eu/dnetlib/dhp/oa/graph/clean/cfhb/masterduplicate.json")
                                                .toURI())
                                .toFile(),
                        testBaseTmpPath.resolve("workingDir").resolve("masterduplicate").toFile());

        graphInputPath = testBaseTmpPath.resolve("input").resolve("graph").toString();
        graphOutputPath = testBaseTmpPath.resolve("output").resolve("graph").toString();
        dsMasterDuplicatePath = testBaseTmpPath.resolve("workingDir").resolve("masterduplicate").toString();



        workingDir = Files.createTempDirectory(CleanGraphSparkJobTest.class.getSimpleName());
        log.info("using work dir {}", workingDir);

        SparkConf conf = new SparkConf();
        conf.setAppName(CleanGraphSparkJobTest.class.getSimpleName());

        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.sql.warehouse.dir", workingDir.toString());
        conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

        spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
    }

    @BeforeEach
    public void setUp() throws ISLookUpException, IOException {
        lenient().when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARIES_XQUERY)).thenReturn(vocs());
        lenient()
                .when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARY_SYNONYMS_XQUERY))
                .thenReturn(synonyms());

        vocabularies = VocabularyGroup.loadVocsFromIS(isLookUpService);
        mapping = CleaningRuleMap.create(vocabularies);
    }

    @AfterAll
    public static void afterAll() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
        spark.stop();
    }

    @Test
    void testCleanRelations() throws Exception {

        spark.read()
                .textFile(graphInputPath.toString() + "/relation")
                .map(as(Relation.class), Encoders.bean(Relation.class))
                .collectAsList()
                .forEach(r -> assertFalse(vocabularies.getTerms(ModelConstants.DNET_RELATION_RELCLASS).contains(r.getRelClass())));

        new CleanGraphSparkJob(
                args("/eu/dnetlib/dhp/oa/graph/input_clean_graph_parameters.json",
                new String[] {
                        "--inputPath", graphInputPath.toString() + "/relation",
                        "--outputPath", graphOutputPath.toString() + "/relation",
                        "--isLookupUrl", "lookupurl",
                        "--graphTableClassName", Relation.class.getCanonicalName(),
                        "--deepClean", "false"
        })).run(false, isLookUpService);

        spark.read()
                .textFile(graphOutputPath.toString() + "/relation")
                .map(as(Relation.class), Encoders.bean(Relation.class))
                .collectAsList()
                .forEach(r -> {

                    assertTrue(vocabularies.getTerms(ModelConstants.DNET_RELATION_RELCLASS).contains(r.getRelClass()));
                    assertTrue(vocabularies.getTerms(ModelConstants.DNET_RELATION_SUBRELTYPE).contains(r.getSubRelType()));

                    assertEquals("iis", r.getDataInfo().getProvenanceaction().getClassid());
                    assertEquals("Inferred by OpenAIRE", r.getDataInfo().getProvenanceaction().getClassname());
                });
    }

    @Test
    void testFilter_invisible_true() throws Exception {

        assertNotNull(vocabularies);
        assertNotNull(mapping);

        String json = IOUtils
                .toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/result_invisible.json"));
        Publication p_in = MAPPER.readValue(json, Publication.class);

        assertTrue(p_in instanceof Result);
        assertTrue(p_in instanceof Publication);

        assertEquals(true, GraphCleaningFunctions.filter(p_in));
    }

    @Test
    void testFilter_true_nothing_to_filter() throws Exception {

        assertNotNull(vocabularies);
        assertNotNull(mapping);

        String json = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/result.json"));
        Publication p_in = MAPPER.readValue(json, Publication.class);

        assertTrue(p_in instanceof Result);
        assertTrue(p_in instanceof Publication);

        assertEquals(true, GraphCleaningFunctions.filter(p_in));
    }

    @Test
    void testFilter_missing_invisible() throws Exception {

        assertNotNull(vocabularies);
        assertNotNull(mapping);

        String json = IOUtils
                .toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/result_missing_invisible.json"));
        Publication p_in = MAPPER.readValue(json, Publication.class);

        assertTrue(p_in instanceof Result);
        assertTrue(p_in instanceof Publication);

        assertEquals(true, GraphCleaningFunctions.filter(p_in));
    }

    @Test
    void testCleaning_publication() throws Exception {

        spark.read()
                .textFile(graphInputPath.toString() + "/publication")
                .map(as(Publication.class), Encoders.bean(Publication.class))
                .collectAsList()
                .forEach(p -> {
                    assertNull(p.getBestaccessright());
                    assertTrue(p instanceof Result);
                    assertTrue(p instanceof Publication);
                });
        
        new CleanGraphSparkJob(
                args("/eu/dnetlib/dhp/oa/graph/input_clean_graph_parameters.json",
                        new String[] {
                                "--inputPath", graphInputPath.toString() + "/publication",
                                "--outputPath", graphOutputPath.toString() + "/publication",
                                "--isLookupUrl", "lookupurl",
                                "--graphTableClassName", Publication.class.getCanonicalName(),
                                "--deepClean", "false"
                        })).run(false, isLookUpService);

        Publication p = spark.read()
                .textFile(graphOutputPath.toString() + "/publication")
                .map(as(Publication.class), Encoders.bean(Publication.class))
                .first();

        assertNull(p.getPublisher());

        assertEquals("und", p.getLanguage().getClassid());
        assertEquals("Undetermined", p.getLanguage().getClassname());

        assertEquals("DE", p.getCountry().get(0).getClassid());
        assertEquals("Germany", p.getCountry().get(0).getClassname());

        assertEquals("0018", p.getInstance().get(0).getInstancetype().getClassid());
        assertEquals("Annotation", p.getInstance().get(0).getInstancetype().getClassname());

        assertEquals("0027", p.getInstance().get(1).getInstancetype().getClassid());
        assertEquals("Model", p.getInstance().get(1).getInstancetype().getClassname());

        assertEquals("0038", p.getInstance().get(2).getInstancetype().getClassid());
        assertEquals("Other literature type", p.getInstance().get(2).getInstancetype().getClassname());

        assertEquals("CLOSED", p.getInstance().get(0).getAccessright().getClassid());
        assertEquals("Closed Access", p.getInstance().get(0).getAccessright().getClassname());

        Set<String> pidTerms = vocabularies.getTerms(ModelConstants.DNET_PID_TYPES);
        assertTrue(
                p
                        .getPid()
                        .stream()
                        .map(StructuredProperty::getQualifier)
                        .allMatch(q -> pidTerms.contains(q.getClassid())));

        List<Instance> poi = p.getInstance();
        assertNotNull(poi);
        assertEquals(3, poi.size());

        final Instance poii = poi.get(0);
        assertNotNull(poii);
        assertNotNull(poii.getPid());

        assertEquals(2, poii.getPid().size());

        assertTrue(
                poii.getPid().stream().anyMatch(s -> s.getValue().equals("10.1007/s109090161569x")));
        assertTrue(poii.getPid().stream().anyMatch(s -> s.getValue().equals("10.1008/abcd")));

        assertNotNull(poii.getAlternateIdentifier());
        assertEquals(1, poii.getAlternateIdentifier().size());

        assertTrue(
                poii
                        .getAlternateIdentifier()
                        .stream()
                        .anyMatch(s -> s.getValue().equals("10.1009/qwerty")));

        assertEquals(3, p.getTitle().size());


        List<String> titles = p
                .getTitle()
                .stream()
                .map(StructuredProperty::getValue)
                .collect(Collectors.toList());
        assertTrue(titles.contains("omic"));
        assertTrue(
                titles.contains("Optical response of strained- and unstrained-silicon cold-electron bolometers test"));
        assertTrue(titles.contains("｢マキャベリ的知性と心の理論の進化論｣ リチャード・バーン， アンドリュー・ホワイトゥン 編／藤田和生， 山下博志， 友永雅巳 監訳"));

        assertEquals("CLOSED", p.getBestaccessright().getClassid());
        assertNull(p.getPublisher());

        assertEquals("1970-10-07", p.getDateofacceptance().getValue());

        assertEquals("0038", p.getInstance().get(2).getInstancetype().getClassid());
        assertEquals("Other literature type", p.getInstance().get(2).getInstancetype().getClassname());

        final List<Instance> pci = p.getInstance();
        assertNotNull(pci);
        assertEquals(3, pci.size());

        final Instance pcii = pci.get(0);
        assertNotNull(pcii);
        assertNotNull(pcii.getPid());

        assertEquals(2, pcii.getPid().size());

        assertTrue(
                pcii.getPid().stream().anyMatch(s -> s.getValue().equals("10.1007/s109090161569x")));
        assertTrue(pcii.getPid().stream().anyMatch(s -> s.getValue().equals("10.1008/abcd")));

        assertNotNull(pcii.getAlternateIdentifier());
        assertEquals(1, pcii.getAlternateIdentifier().size());
        assertTrue(
                pcii
                        .getAlternateIdentifier()
                        .stream()
                        .anyMatch(s -> s.getValue().equals("10.1009/qwerty")));

        assertNotNull(p.getSubject());

        List<Subject> fos_subjects = p
                .getSubject()
                .stream()
                .filter(s -> ModelConstants.DNET_SUBJECT_FOS_CLASSID.equals(s.getQualifier().getClassid()))
                .collect(Collectors.toList());

        assertNotNull(fos_subjects);
        assertEquals(2, fos_subjects.size());

        assertTrue(
                fos_subjects
                        .stream()
                        .anyMatch(
                                s -> "0101 mathematics".equals(s.getValue()) &
                                        ModelConstants.DNET_SUBJECT_FOS_CLASSID.equals(s.getQualifier().getClassid()) &
                                        "sysimport:crosswalk:datasetarchive"
                                                .equals(s.getDataInfo().getProvenanceaction().getClassid())));

        assertTrue(
                fos_subjects
                        .stream()
                        .anyMatch(
                                s -> "0102 computer and information sciences".equals(s.getValue()) &
                                        ModelConstants.DNET_SUBJECT_FOS_CLASSID.equals(s.getQualifier().getClassid())));

        verify_keyword(p, "In Situ Hybridization");
        verify_keyword(p, "Avicennia");
    }

    private List<String> vocs() throws IOException {
        return IOUtils
                .readLines(
                        GraphCleaningFunctionsTest.class.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/terms.txt"));
    }

    private List<String> synonyms() throws IOException {
        return IOUtils
                .readLines(
                        GraphCleaningFunctionsTest.class.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/synonyms.txt"));
    }

    private static <R> MapFunction<String, R> as(Class<R> clazz) {
        return s -> MAPPER.readValue(s, clazz);
    }

    private static String classPathResourceAsString(String path) throws IOException {
        return IOUtils
                .toString(
                        CleanGraphSparkJobTest.class
                                .getResourceAsStream(path));
    }

    private ArgumentApplicationParser args(String paramSpecs, String[] args) throws IOException, ParseException {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(classPathResourceAsString(paramSpecs));
        parser.parseArgument(args);
        return parser;
    }

    private static void verify_keyword(Publication p_cleaned, String subject) {
        Optional<Subject> s1 = p_cleaned
                .getSubject()
                .stream()
                .filter(s -> s.getValue().equals(subject))
                .findFirst();

        assertTrue(s1.isPresent());
        assertEquals(ModelConstants.DNET_SUBJECT_KEYWORD, s1.get().getQualifier().getClassid());
        assertEquals(ModelConstants.DNET_SUBJECT_KEYWORD, s1.get().getQualifier().getClassname());
    }

    private Stream<StructuredProperty> getAuthorPids(Result pub) {
        return pub
                .getAuthor()
                .stream()
                .map(Author::getPid)
                .flatMap(Collection::stream);
    }

}
