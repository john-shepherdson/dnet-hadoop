
package eu.dnetlib.dhp.oa.graph.clean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.lenient;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.GraphCleaningFunctions;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
public class CleanGraphSparkJobTest {

	private static final Logger log = LoggerFactory.getLogger(CleanGraphSparkJobTest.class);

	public static final ObjectMapper MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	@Mock
	private ISLookUpService isLookUpService;

	private VocabularyGroup vocabularies;

	private CleaningRuleMap mapping;

	private static SparkSession spark;

	private static Path testBaseTmpPath;

	private static String graphInputPath;

	private static String graphOutputPath;

	private static String dsMasterDuplicatePath;

	@BeforeAll
	public static void beforeAll() throws IOException, URISyntaxException {
		testBaseTmpPath = Files.createTempDirectory(CleanGraphSparkJobTest.class.getSimpleName());
		log.info("using test base path {}", testBaseTmpPath);

		File basePath = Paths
			.get(
				Objects
					.requireNonNull(
						CleanGraphSparkJobTest.class.getResource("/eu/dnetlib/dhp/oa/graph/clean/graph"))
					.toURI())
			.toFile();

		List<File> paths = FileUtils
			.listFilesAndDirs(basePath, FalseFileFilter.FALSE, TrueFileFilter.TRUE)
			.stream()
			.filter(f -> !f.getAbsolutePath().endsWith("/graph"))
			.collect(Collectors.toList());

		for (File path : paths) {
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

		SparkConf conf = new SparkConf();
		conf.setAppName(CleanGraphSparkJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", testBaseTmpPath.toString());
		conf.set("hive.metastore.warehouse.dir", testBaseTmpPath.resolve("warehouse").toString());

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
		FileUtils.deleteDirectory(testBaseTmpPath.toFile());
		spark.stop();
	}

	@Test
	void testCleanRelations() throws Exception {

		spark
			.read()
			.textFile(graphInputPath.toString() + "/relation")
			.map(as(Relation.class), Encoders.bean(Relation.class))
			.collectAsList()
			.forEach(
				r -> assertFalse(
					vocabularies.getTerms(ModelConstants.DNET_RELATION_RELCLASS).contains(r.getRelClass())));

		new CleanGraphSparkJob(
			args(
				"/eu/dnetlib/dhp/oa/graph/input_clean_graph_parameters.json",
				new String[] {
					"--inputPath", graphInputPath + "/relation",
					"--outputPath", graphOutputPath + "/relation",
					"--isLookupUrl", "lookupurl",
					"--graphTableClassName", Relation.class.getCanonicalName(),
					"--deepClean", "false",
					"--masterDuplicatePath", dsMasterDuplicatePath,
				})).run(false, isLookUpService);

		spark
			.read()
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
			.toString(
				Objects
					.requireNonNull(
						getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/result_invisible.json")));
		Publication p_in = MAPPER.readValue(json, Publication.class);

		assertTrue(p_in instanceof Result);
		assertTrue(p_in instanceof Publication);

		assertEquals(true, GraphCleaningFunctions.filter(p_in));
	}

	@Test
	void testFilter_true_nothing_to_filter() throws Exception {

		assertNotNull(vocabularies);
		assertNotNull(mapping);

		String json = IOUtils
			.toString(
				Objects
					.requireNonNull(
						getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/result.json")));
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
			.toString(
				Objects
					.requireNonNull(
						getClass()
							.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/result_missing_invisible.json")));
		Publication p_in = MAPPER.readValue(json, Publication.class);

		assertTrue(p_in instanceof Result);
		assertTrue(p_in instanceof Publication);

		assertEquals(true, GraphCleaningFunctions.filter(p_in));
	}

	@Test
	void testCleaning_publication() throws Exception {

		final String id = "50|CSC_________::2250a70c903c6ac6e4c01438259e9375";

		Publication p_in = read(spark, graphInputPath + "/publication", Publication.class)
			.filter(String.format("id = '%s'", id))
			.first();

		assertNull(p_in.getBestaccessright());
		assertTrue(p_in instanceof Result);
		assertTrue(p_in instanceof Publication);

		new CleanGraphSparkJob(
			args(
				"/eu/dnetlib/dhp/oa/graph/input_clean_graph_parameters.json",
				new String[] {
					"--inputPath", graphInputPath + "/publication",
					"--outputPath", graphOutputPath + "/publication",
					"--isLookupUrl", "lookupurl",
					"--graphTableClassName", Publication.class.getCanonicalName(),
					"--deepClean", "false",
					"--masterDuplicatePath", dsMasterDuplicatePath,
				})).run(false, isLookUpService);

		Publication p = read(spark, graphOutputPath + "/publication", Publication.class)
			.filter(String.format("id = '%s'", id))
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

	@Test
	void testCleanDoiBoost() throws IOException, ParseException, ISLookUpException, ClassNotFoundException {
		verifyFiltering(1, "50|doi_________::b0baa0eb88a5788f0b8815560d2a32f2");
	}

	@Test
	void testCleanDoiBoost2() throws IOException, ParseException, ISLookUpException, ClassNotFoundException {
		verifyFiltering(1, "50|doi_________::4972b0ca81b96b225aed8038bb965656");
	}

	private void verifyFiltering(int expectedCount, String id)
		throws ISLookUpException, ClassNotFoundException, IOException, ParseException {
		new CleanGraphSparkJob(
			args(
				"/eu/dnetlib/dhp/oa/graph/input_clean_graph_parameters.json",
				new String[] {
					"--inputPath", graphInputPath + "/publication",
					"--outputPath", graphOutputPath + "/publication",
					"--isLookupUrl", "lookupurl",
					"--graphTableClassName", Publication.class.getCanonicalName(),
					"--deepClean", "false",
					"--masterDuplicatePath", dsMasterDuplicatePath,
				})).run(false, isLookUpService);

		Dataset<Publication> p = read(spark, graphOutputPath + "/publication", Publication.class)
			.filter(String.format("id = '%s'", id));

		assertEquals(expectedCount, p.count());
	}

	@Test
	void testCleanContext() throws Exception {
		final String prefix = "gcube ";

		new CleanGraphSparkJob(
			args(
				"/eu/dnetlib/dhp/oa/graph/input_clean_graph_parameters.json",
				new String[] {
					"--inputPath", graphInputPath + "/publication",
					"--outputPath", graphOutputPath + "/publication",
					"--isLookupUrl", "lookupurl",
					"--graphTableClassName", Publication.class.getCanonicalName(),
					"--deepClean", "true",
					"--contextId", "sobigdata",
					"--verifyParam", "gCube ",
					"--masterDuplicatePath", dsMasterDuplicatePath,
					"--country", "NL",
					"--verifyCountryParam", "10.17632",
					"--collectedfrom", "NARCIS",
					"--hostedBy", Objects
						.requireNonNull(
							getClass()
								.getResource("/eu/dnetlib/dhp/oa/graph/clean/hostedBy"))
						.getPath()
				})).run(false, isLookUpService);

		Dataset<Publication> pubs = read(spark, graphOutputPath + "/publication", Publication.class)
			.filter((FilterFunction<Publication>) p1 -> StringUtils.endsWith(p1.getId(), "_ctx"));

		assertEquals(7, pubs.count());

		// original result with sobigdata context and gcube as starting string in the main title for the publication
		assertEquals(
			0,
			pubs
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::0224aae28af558f21768dbc6439a_ctx"))
				.first()
				.getContext()
				.size());

		// original result with sobigdata context without gcube as starting string in the main title for the publication
		assertEquals(
			1,
			pubs
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::20c414a3b1c742d5dd3851f1b67d_ctx"))
				.first()
				.getContext()
				.size());
		assertEquals(
			"sobigdata::projects::2",
			pubs
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::20c414a3b1c742d5dd3851f1b67d_ctx"))
				.first()
				.getContext()
				.get(0)
				.getId());

		// original result with sobigdata context with gcube as starting string in the subtitle
		assertEquals(
			1,
			pubs
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::3c81248c335f0aa07e06817ece6f_ctx"))
				.first()
				.getContext()
				.size());
		assertEquals(
			"sobigdata::projects::2",
			pubs
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::3c81248c335f0aa07e06817ece6f_ctx"))
				.first()
				.getContext()
				.get(0)
				.getId());

		List<StructuredProperty> titles = pubs
			.filter(
				(FilterFunction<Publication>) p -> p
					.getId()
					.equals("50|DansKnawCris::3c81248c335f0aa07e06817ece6f_ctx"))
			.first()
			.getTitle();

		assertEquals(1, titles.size());
		assertTrue(titles.get(0).getValue().toLowerCase().startsWith(prefix));
		assertEquals("subtitle", titles.get(0).getQualifier().getClassid());

		// original result with sobigdata context with gcube not as starting string in the main title
		assertEquals(
			1,
			pubs
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::3c9f068ddc930360bec6925488a9_ctx"))
				.first()
				.getContext()
				.size());
		assertEquals(
			"sobigdata::projects::1",
			pubs
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::3c9f068ddc930360bec6925488a9_ctx"))
				.first()
				.getContext()
				.get(0)
				.getId());
		titles = pubs
			.filter(
				(FilterFunction<Publication>) p -> p
					.getId()
					.equals("50|DansKnawCris::3c9f068ddc930360bec6925488a9_ctx"))
			.first()
			.getTitle();

		assertEquals(1, titles.size());
		assertFalse(titles.get(0).getValue().toLowerCase().startsWith(prefix));
		assertTrue(titles.get(0).getValue().toLowerCase().contains(prefix.trim()));
		assertEquals("main title", titles.get(0).getQualifier().getClassid());

		// original result with sobigdata in context and also other contexts with gcube as starting string for the main
		// title
		assertEquals(
			1,
			pubs
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::4669a378a73661417182c208e6fd_ctx"))
				.first()
				.getContext()
				.size());
		assertEquals(
			"dh-ch",
			pubs
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::4669a378a73661417182c208e6fd_ctx"))
				.first()
				.getContext()
				.get(0)
				.getId());
		titles = pubs
			.filter(
				(FilterFunction<Publication>) p -> p
					.getId()
					.equals("50|DansKnawCris::4669a378a73661417182c208e6fd_ctx"))
			.first()
			.getTitle();

		assertEquals(1, titles.size());
		assertTrue(titles.get(0).getValue().toLowerCase().startsWith(prefix));
		assertEquals("main title", titles.get(0).getQualifier().getClassid());

		// original result with multiple main title one of which whith gcube as starting string and with 2 contextes
		assertEquals(
			1,
			pubs
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::4a9152e80f860eab99072e921d74_ctx"))
				.first()
				.getContext()
				.size());
		assertEquals(
			"dh-ch",
			pubs
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::4a9152e80f860eab99072e921d74_ctx"))
				.first()
				.getContext()
				.get(0)
				.getId());
		titles = pubs
			.filter(
				(FilterFunction<Publication>) p -> p
					.getId()
					.equals("50|DansKnawCris::4a9152e80f860eab99072e921d74_ctx"))
			.first()
			.getTitle();

		assertEquals(2, titles.size());
		assertTrue(
			titles
				.stream()
				.anyMatch(
					t -> t.getQualifier().getClassid().equals("main title")
						&& t.getValue().toLowerCase().startsWith(prefix)));

		// original result without sobigdata in context with gcube as starting string for the main title
		assertEquals(
			1,
			pubs
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|dedup_wf_001::01e6a28565ca01376b7548e530c6_ctx"))
				.first()
				.getContext()
				.size());
		assertEquals(
			"dh-ch",
			pubs
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|dedup_wf_001::01e6a28565ca01376b7548e530c6_ctx"))
				.first()
				.getContext()
				.get(0)
				.getId());
		titles = pubs
			.filter(
				(FilterFunction<Publication>) p -> p
					.getId()
					.equals("50|dedup_wf_001::01e6a28565ca01376b7548e530c6_ctx"))
			.first()
			.getTitle();

		assertEquals(2, titles.size());

		assertTrue(
			titles
				.stream()
				.anyMatch(
					t -> t.getQualifier().getClassid().equals("main title")
						&& t.getValue().toLowerCase().startsWith(prefix)));

	}

	@Test
	void testCleanCfHbSparkJob() throws Exception {

		final Dataset<Publication> pubs_in = read(spark, graphInputPath + "/publication", Publication.class);
		final Publication p1_in = pubs_in
			.filter("id = '50|doi_________::09821844208a5cd6300b2bfb13b_cfhb'")
			.first();
		assertEquals("10|re3data_____::4c4416659cb74c2e0e891a883a047cbc", p1_in.getCollectedfrom().get(0).getKey());
		assertEquals("Bacterial Protein Interaction Database - DUP", p1_in.getCollectedfrom().get(0).getValue());
		assertEquals(
			"10|re3data_____::4c4416659cb74c2e0e891a883a047cbc",
			p1_in.getInstance().get(0).getCollectedfrom().getKey());
		assertEquals(
			"Bacterial Protein Interaction Database - DUP", p1_in.getInstance().get(0).getCollectedfrom().getValue());

		final Publication p2_in = pubs_in
			.filter("id = '50|DansKnawCris::0dd644304b7116e8e58da3a5e3a_cfhb'")
			.first();
		assertEquals("10|opendoar____::788b4ac1e172d8e520c2b9461c0a3d35", p2_in.getCollectedfrom().get(0).getKey());
		assertEquals("FILUR DATA - DUP", p2_in.getCollectedfrom().get(0).getValue());
		assertEquals(
			"10|opendoar____::788b4ac1e172d8e520c2b9461c0a3d35",
			p2_in.getInstance().get(0).getCollectedfrom().getKey());
		assertEquals("FILUR DATA - DUP", p2_in.getInstance().get(0).getCollectedfrom().getValue());
		assertEquals(
			"10|re3data_____::6ffd7bc058f762912dc494cd9c175341", p2_in.getInstance().get(0).getHostedby().getKey());
		assertEquals("depositar - DUP", p2_in.getInstance().get(0).getHostedby().getValue());

		final Publication p3_in = pubs_in
			.filter("id = '50|DansKnawCris::203a27996ddc0fd1948258e5b7d_cfhb'")
			.first();
		assertEquals("10|openaire____::c6df70599aa984f16ee52b4b86d2e89f", p3_in.getCollectedfrom().get(0).getKey());
		assertEquals("DANS (Data Archiving and Networked Services)", p3_in.getCollectedfrom().get(0).getValue());
		assertEquals(
			"10|openaire____::c6df70599aa984f16ee52b4b86d2e89f",
			p3_in.getInstance().get(0).getCollectedfrom().getKey());
		assertEquals(
			"DANS (Data Archiving and Networked Services)", p3_in.getInstance().get(0).getCollectedfrom().getValue());
		assertEquals(
			"10|openaire____::c6df70599aa984f16ee52b4b86d2e89f", p3_in.getInstance().get(0).getHostedby().getKey());
		assertEquals(
			"DANS (Data Archiving and Networked Services)", p3_in.getInstance().get(0).getHostedby().getValue());

		new CleanGraphSparkJob(
			args(
				"/eu/dnetlib/dhp/oa/graph/input_clean_graph_parameters.json",
				new String[] {
					"--inputPath", graphInputPath + "/publication",
					"--outputPath", graphOutputPath + "/publication",
					"--isLookupUrl", "lookupurl",
					"--graphTableClassName", Publication.class.getCanonicalName(),
					"--deepClean", "true",
					"--contextId", "sobigdata",
					"--verifyParam", "gCube ",
					"--masterDuplicatePath", dsMasterDuplicatePath,
					"--country", "NL",
					"--verifyCountryParam", "10.17632",
					"--collectedfrom", "NARCIS",
					"--hostedBy", Objects
						.requireNonNull(
							getClass()
								.getResource("/eu/dnetlib/dhp/oa/graph/clean/hostedBy"))
						.getPath()
				})).run(false, isLookUpService);

		assertTrue(Files.exists(Paths.get(graphOutputPath, "publication")));

		final Dataset<Publication> pubs_out = read(spark, graphOutputPath + "/publication", Publication.class)
			.filter((FilterFunction<Publication>) p -> StringUtils.endsWith(p.getId(), "_cfhb"));

		assertEquals(3, pubs_out.count());

		final Publication p1_out = pubs_out
			.filter("id = '50|doi_________::09821844208a5cd6300b2bfb13b_cfhb'")
			.first();
		assertEquals("10|fairsharing_::a29d1598024f9e87beab4b98411d48ce", p1_out.getCollectedfrom().get(0).getKey());
		assertEquals("Bacterial Protein Interaction Database", p1_out.getCollectedfrom().get(0).getValue());
		assertEquals(
			"10|fairsharing_::a29d1598024f9e87beab4b98411d48ce",
			p1_out.getInstance().get(0).getCollectedfrom().getKey());
		assertEquals(
			"Bacterial Protein Interaction Database", p1_out.getInstance().get(0).getCollectedfrom().getValue());

		final Publication p2_out = pubs_out
			.filter("id = '50|DansKnawCris::0dd644304b7116e8e58da3a5e3a_cfhb'")
			.first();
		assertEquals("10|re3data_____::fc1db64b3964826913b1e9eafe830490", p2_out.getCollectedfrom().get(0).getKey());
		assertEquals("FULIR Data", p2_out.getCollectedfrom().get(0).getValue());
		assertEquals(
			"10|re3data_____::fc1db64b3964826913b1e9eafe830490",
			p2_out.getInstance().get(0).getCollectedfrom().getKey());
		assertEquals("FULIR Data", p2_out.getInstance().get(0).getCollectedfrom().getValue());
		assertEquals(
			"10|fairsharing_::3f647cadf56541fb9513cb63ec370187", p2_out.getInstance().get(0).getHostedby().getKey());
		assertEquals("depositar", p2_out.getInstance().get(0).getHostedby().getValue());

		final Publication p3_out = pubs_out
			.filter("id = '50|DansKnawCris::203a27996ddc0fd1948258e5b7d_cfhb'")
			.first();
		assertEquals("10|openaire____::c6df70599aa984f16ee52b4b86d2e89f", p3_out.getCollectedfrom().get(0).getKey());
		assertEquals("DANS (Data Archiving and Networked Services)", p3_out.getCollectedfrom().get(0).getValue());
		assertEquals(
			"10|openaire____::c6df70599aa984f16ee52b4b86d2e89f",
			p3_out.getInstance().get(0).getCollectedfrom().getKey());
		assertEquals(
			"DANS (Data Archiving and Networked Services)", p3_out.getInstance().get(0).getCollectedfrom().getValue());
		assertEquals(
			"10|openaire____::c6df70599aa984f16ee52b4b86d2e89f", p3_out.getInstance().get(0).getHostedby().getKey());
		assertEquals(
			"DANS (Data Archiving and Networked Services)", p3_out.getInstance().get(0).getHostedby().getValue());
	}

	@Test
	void testCleanCountry() throws Exception {

		new CleanGraphSparkJob(
			args(
				"/eu/dnetlib/dhp/oa/graph/input_clean_graph_parameters.json",
				new String[] {
					"--inputPath", graphInputPath + "/publication",
					"--outputPath", graphOutputPath + "/publication",
					"--isLookupUrl", "lookupurl",
					"--graphTableClassName", Publication.class.getCanonicalName(),
					"--deepClean", "true",
					"--contextId", "sobigdata",
					"--verifyParam", "gCube ",
					"--masterDuplicatePath", dsMasterDuplicatePath,
					"--country", "NL",
					"--verifyCountryParam", "10.17632",
					"--collectedfrom", "NARCIS",
					"--hostedBy", Objects
						.requireNonNull(
							getClass()
								.getResource("/eu/dnetlib/dhp/oa/graph/clean/hostedBy"))
						.getPath()
				})).run(false, isLookUpService);

		final Dataset<Publication> pubs_out = read(spark, graphOutputPath + "/publication", Publication.class)
			.filter((FilterFunction<Publication>) p -> StringUtils.endsWith(p.getId(), "_country"));

		assertEquals(8, pubs_out.count());

		// original result with NL country and doi starting with Mendely prefix, but not collectedfrom NARCIS
		assertEquals(
			1,
			pubs_out
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::0224aae28af558f21768dbc6_country"))
				.first()
				.getCountry()
				.size());

		// original result with NL country and pid not starting with Mendely prefix
		assertEquals(
			1,
			pubs_out
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::20c414a3b1c742d5dd3851f1_country"))
				.first()
				.getCountry()
				.size());

		// original result with NL country and doi starting with Mendely prefix and collectedfrom NARCIS but not
		// inserted with propagation
		assertEquals(
			1,
			pubs_out
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::3c81248c335f0aa07e06817e_country"))
				.first()
				.getCountry()
				.size());

		// original result with NL country and doi starting with Mendely prefix and collectedfrom NARCIS inserted with
		// propagation
		assertEquals(
			0,
			pubs_out
				.filter(
					(FilterFunction<Publication>) p -> p
						.getId()
						.equals("50|DansKnawCris::3c81248c335f0aa07e06817d_country"))
				.first()
				.getCountry()
				.size());
	}

	private List<String> vocs() throws IOException {
		return IOUtils
			.readLines(
				Objects
					.requireNonNull(
						getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/terms.txt")));
	}

	private List<String> synonyms() throws IOException {
		return IOUtils
			.readLines(
				Objects
					.requireNonNull(
						getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/synonyms.txt")));
	}

	private <R> org.apache.spark.sql.Dataset<R> read(SparkSession spark, String path, Class<R> clazz) {
		return spark
			.read()
			.textFile(path)
			.map(as(clazz), Encoders.bean(clazz));
	}

	private static <R> MapFunction<String, R> as(Class<R> clazz) {
		return s -> MAPPER.readValue(s, clazz);
	}

	private static String classPathResourceAsString(String path) throws IOException {
		return IOUtils
			.toString(
				Objects
					.requireNonNull(
						CleanGraphSparkJobTest.class.getResourceAsStream(path)));
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

}
