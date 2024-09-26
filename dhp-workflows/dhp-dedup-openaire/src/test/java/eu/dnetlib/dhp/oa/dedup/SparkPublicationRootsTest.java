
package eu.dnetlib.dhp.oa.dedup;

import static java.nio.file.Files.createTempDirectory;

import static org.apache.spark.sql.functions.count;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.lenient;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SparkPublicationRootsTest implements Serializable {

	@Mock(serializable = true)
	ISLookUpService isLookUpService;

	private static SparkSession spark;
	private static String workingPath;

	private static String graphInputPath;
	private static String graphOutputPath;
	private static final String testActionSetId = "test-orchestrator";

	private static Path testBaseTmpPath;

	private static final ObjectMapper MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	@BeforeAll
	public static void init() throws IOException, URISyntaxException {

		testBaseTmpPath = createTempDirectory(SparkPublicationRootsTest.class.getSimpleName() + "-");

		final File entitiesSources = Paths
			.get(SparkPublicationRootsTest.class.getResource("/eu/dnetlib/dhp/dedup/root").toURI())
			.toFile();

		FileUtils
			.copyDirectory(
				entitiesSources,
				testBaseTmpPath.resolve("input").toFile());

		workingPath = testBaseTmpPath.resolve("workingPath").toString();
		graphInputPath = testBaseTmpPath.resolve("input").resolve("entities").toString();
		graphOutputPath = testBaseTmpPath.resolve("output").toString();

		FileUtils.deleteDirectory(new File(workingPath));
		FileUtils.deleteDirectory(new File(graphOutputPath));

		final SparkConf conf = new SparkConf();
		conf.set("spark.sql.shuffle.partitions", "10");
		spark = SparkSession
			.builder()
			.appName(SparkPublicationRootsTest.class.getSimpleName())
			.master("local[*]")
			.config(conf)
			.getOrCreate();
	}

	@BeforeEach
	public void setUp() throws IOException, ISLookUpException {

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains(testActionSetId)))
			.thenReturn(classPathResourceAsString("/eu/dnetlib/dhp/dedup/profiles/mock_orchestrator_publication.xml"));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("publication")))
			.thenReturn(classPathResourceAsString("/eu/dnetlib/dhp/dedup/conf/pub.curr.conf.json"));
	}

	@AfterAll
	public static void tearDown() throws IOException {
		FileUtils.deleteDirectory(testBaseTmpPath.toFile());
		spark.close();
	}

	@Test
	@Order(1)
	void createSimRelsTest() throws Exception {
		new SparkCreateSimRels(args(
			"/eu/dnetlib/dhp/oa/dedup/createSimRels_parameters.json",
			new String[] {
				"--graphBasePath", graphInputPath,
				"--actionSetId", testActionSetId,
				"--isLookUpUrl", "lookupurl",
				"--workingPath", workingPath,
				"--numPartitions", "5"
			}), spark)
				.run(isLookUpService);

		long pubs_simrel = spark
			.read()
			.load(DedupUtility.createSimRelPath(workingPath, testActionSetId, "publication"))
			.count();

		assertEquals(9, pubs_simrel);
	}

	@Test
	@Order(2)
	void cutMergeRelsTest() throws Exception {
		new SparkCreateMergeRels(args(
			"/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json",
			new String[] {
				"--graphBasePath", graphInputPath,
				"--actionSetId", testActionSetId,
				"--isLookUpUrl", "lookupurl",
				"--workingPath", workingPath,
				"--cutConnectedComponent", "3",
				"-h", ""
			}), spark)
				.run(isLookUpService);

		long pubs_mergerel = spark
			.read()
			.load(workingPath + "/" + testActionSetId + "/publication_mergerel")
			.as(Encoders.bean(Relation.class))
			.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase("merges"))
			.groupBy("source")
			.agg(count("target").alias("cnt"))
			.select("source", "cnt")
			.where("cnt > 3")
			.count();

		assertEquals(0, pubs_mergerel);

		FileUtils.deleteDirectory(new File(workingPath + "/" + testActionSetId + "/publication_mergerel"));
	}

	@Test
	@Order(3)
	void createMergeRelsTest() throws Exception {
		new SparkCreateMergeRels(args(
			"/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json",
			new String[] {
				"--graphBasePath", graphInputPath,
				"--actionSetId", testActionSetId,
				"--isLookUpUrl", "lookupurl",
				"--workingPath", workingPath,
				"-h", ""
			}), spark)
				.run(isLookUpService);

		final Dataset<Relation> merges = spark
			.read()
			.load(workingPath + "/" + testActionSetId + "/publication_mergerel")
			.as(Encoders.bean(Relation.class));

		final List<Relation> mergeList = merges
			.filter("source == '50|doi_dedup___::d5021b53204e4fdeab6ff5d5bc468032'")
			.collectAsList();
		assertEquals(3, mergeList.size());
		Set<String> dups = Sets
			.newHashSet(
				"50|doi_________::3b1d0d8e8f930826665df9d6b82fbb73",
				"50|doi_________::d5021b53204e4fdeab6ff5d5bc468032",
				"50|arXiv_______::c93aeb433eb90ed7a86e29be00791b7c");
		mergeList.forEach(r -> {
			assertEquals(ModelConstants.RESULT_RESULT, r.getRelType());
			assertEquals(ModelConstants.DEDUP, r.getSubRelType());
			assertEquals(ModelConstants.MERGES, r.getRelClass());
			assertTrue(dups.contains(r.getTarget()));
		});

		final List<Relation> mergedIn = merges
			.filter("target == '50|doi_dedup___::d5021b53204e4fdeab6ff5d5bc468032'")
			.collectAsList();
		assertEquals(3, mergedIn.size());
		mergedIn.forEach(r -> {
			assertEquals(ModelConstants.RESULT_RESULT, r.getRelType());
			assertEquals(ModelConstants.DEDUP, r.getSubRelType());
			assertEquals(ModelConstants.IS_MERGED_IN, r.getRelClass());
			assertTrue(dups.contains(r.getSource()));
		});

		assertEquals(26, merges.count());
	}

	@Test
	@Order(4)
	void createDedupRecordTest() throws Exception {
		new SparkCreateDedupRecord(args(
			"/eu/dnetlib/dhp/oa/dedup/createDedupRecord_parameters.json",
			new String[] {
				"--graphBasePath", graphInputPath,
				"--actionSetId", testActionSetId,
				"--isLookUpUrl", "lookupurl",
				"--workingPath", workingPath
			}), spark)
				.run(isLookUpService);

		final Dataset<Publication> roots = spark
			.read()
			.textFile(workingPath + "/" + testActionSetId + "/publication_deduprecord")
			.map(asEntity(Publication.class), Encoders.bean(Publication.class));

		assertEquals(4, roots.count());

		final Dataset<Publication> pubs = spark
			.read()
			.textFile(DedupUtility.createEntityPath(graphInputPath, "publication"))
			.map(asEntity(Publication.class), Encoders.bean(Publication.class));

		verifyRoot_case_1(roots, pubs);
		verifyRoot_case_2(roots, pubs);
	}

	private static void verifyRoot_case_1(Dataset<Publication> roots, Dataset<Publication> pubs) {
		Publication root = roots
			.filter("id = '50|doi_dedup___::d5021b53204e4fdeab6ff5d5bc468032'")
			.first();
		assertNotNull(root);

		Publication crossref_duplicate = pubs
			.filter("id = '50|doi_________::d5021b53204e4fdeab6ff5d5bc468032'")
			.collectAsList()
			.get(0);

		assertEquals(crossref_duplicate.getJournal().getName(), root.getJournal().getName());
		assertEquals(crossref_duplicate.getJournal().getIssnPrinted(), root.getJournal().getIssnPrinted());
		assertEquals(crossref_duplicate.getPublisher().getValue(), root.getPublisher().getValue());

		Set<String> rootPids = root
			.getPid()
			.stream()
			.map(StructuredProperty::getValue)
			.collect(Collectors.toCollection(HashSet::new));
		Set<String> dupPids = crossref_duplicate
			.getPid()
			.stream()
			.map(StructuredProperty::getValue)
			.collect(Collectors.toCollection(HashSet::new));

		assertFalse(Sets.intersection(rootPids, dupPids).isEmpty());
		assertTrue(rootPids.contains("10.1109/jstqe.2022.3205716"));

		Optional<Instance> instance_cr = root
			.getInstance()
			.stream()
			.filter(i -> i.getCollectedfrom().getValue().equals("Crossref"))
			.findFirst();
		assertTrue(instance_cr.isPresent());
		assertEquals("OPEN", instance_cr.get().getAccessright().getClassid());
		assertEquals("Open Access", instance_cr.get().getAccessright().getClassname());
		assertEquals(OpenAccessRoute.hybrid, instance_cr.get().getAccessright().getOpenAccessRoute());
		assertEquals(
			"IEEE Journal of Selected Topics in Quantum Electronics", instance_cr.get().getHostedby().getValue());
		assertEquals("0001", instance_cr.get().getInstancetype().getClassid());
		assertEquals("Article", instance_cr.get().getInstancetype().getClassname());
	}

	private void verifyRoot_case_2(Dataset<Publication> roots, Dataset<Publication> pubs) {
		Publication root = roots
			.filter("id = '50|doi_dedup___::18aff3b55fb6876466a5d4bd82434885'")
			.first();
		assertNotNull(root);

		Publication crossref_duplicate = pubs
			.filter("id = '50|doi_________::18aff3b55fb6876466a5d4bd82434885'")
			.first();

		// System.err.println(new ObjectMapper().writeValueAsString(root));

		assertEquals(crossref_duplicate.getJournal().getName(), root.getJournal().getName());
		assertEquals(crossref_duplicate.getJournal().getIssnOnline(), root.getJournal().getIssnOnline());
		assertEquals(crossref_duplicate.getJournal().getVol(), root.getJournal().getVol());

		assertEquals(crossref_duplicate.getPublisher().getValue(), root.getPublisher().getValue());

		Set<String> dups_cf = pubs
			.collectAsList()
			.stream()
			.flatMap(p -> p.getCollectedfrom().stream())
			.map(KeyValue::getValue)
			.collect(Collectors.toCollection(HashSet::new));

		Set<String> root_cf = root
			.getCollectedfrom()
			.stream()
			.map(KeyValue::getValue)
			.collect(Collectors.toCollection(HashSet::new));

		assertTrue(Sets.difference(root_cf, dups_cf).isEmpty());
	}

	@Test
	@Order(6)
	void updateEntityTest() throws Exception {
		new SparkUpdateEntity(args(
			"/eu/dnetlib/dhp/oa/dedup/updateEntity_parameters.json",
			new String[] {
				"--graphBasePath", graphInputPath,
				"--workingPath", workingPath,
				"--dedupGraphPath", graphOutputPath
			}), spark)
				.run(isLookUpService);

		long publications = spark.read().textFile(graphOutputPath + "/publication").count();

		long mergedPubs = spark
			.read()
			.load(workingPath + "/" + testActionSetId + "/publication_mergerel")
			.as(Encoders.bean(Relation.class))
			.where("relClass=='merges'")
			.map((MapFunction<Relation, String>) Relation::getTarget, Encoders.STRING())
			.distinct()
			.count();

		assertEquals(20, publications); // 16 originals + 3 roots

		long deletedPubs = spark
			.read()
			.textFile(graphOutputPath + "/publication")
			.map(asEntity(Publication.class), Encoders.bean(Publication.class))
			.filter("datainfo.deletedbyinference == true")
			.map((MapFunction<Publication, String>) OafEntity::getId, Encoders.STRING())
			.distinct()
			.count();

//		assertEquals(mergedPubs, deletedPubs);
	}

	private static String classPathResourceAsString(String path) throws IOException {
		return IOUtils
			.toString(
				SparkPublicationRootsTest.class
					.getResourceAsStream(path));
	}

	private static <T extends OafEntity> MapFunction<String, T> asEntity(Class<T> clazz) {
		return value -> MAPPER.readValue(value, clazz);
	}

	private ArgumentApplicationParser args(String paramSpecs, String[] args) throws IOException, ParseException {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(classPathResourceAsString(paramSpecs));
		parser.parseArgument(args);
		return parser;
	}

}
