
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
import org.apache.spark.api.java.JavaSparkContext;
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
public class SparkPublicationRootsTest2 implements Serializable {

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

		testBaseTmpPath = createTempDirectory(SparkPublicationRootsTest2.class.getSimpleName() + "-");

		final File entitiesSources = Paths
			.get(SparkPublicationRootsTest2.class.getResource("/eu/dnetlib/dhp/dedup/root").toURI())
			.toFile();

		FileUtils
			.copyDirectory(
				entitiesSources,
				testBaseTmpPath.resolve("input").toFile());

		FileUtils
			.copyFileToDirectory(
				Paths
					.get(
						SparkPublicationRootsTest2.class
							.getResource(
								"/eu/dnetlib/dhp/dedup/root/alterations/publication/publication_1.gz")
							.toURI())
					.toFile(),
				testBaseTmpPath.resolve("input").resolve("entities").resolve("publication").toFile());

		workingPath = testBaseTmpPath.resolve("workingPath").toString();
		graphInputPath = testBaseTmpPath.resolve("input").resolve("entities").toString();
		graphOutputPath = testBaseTmpPath.resolve("output").toString();

		final SparkConf conf = new SparkConf();
		conf.set("spark.sql.shuffle.partitions", "10");
		spark = SparkSession
			.builder()
			.appName(SparkPublicationRootsTest2.class.getSimpleName())
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
	}

	@Test
	@Order(7)
	void dedupAlteredDatasetTest() throws Exception {

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

		new SparkCreateMergeRels(args(
			"/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json",
			new String[] {
				"--graphBasePath", graphInputPath,
				"--actionSetId", testActionSetId,
				"--isLookUpUrl", "lookupurl",
				"--workingPath", workingPath
			}), spark)
				.run(isLookUpService);

		final Dataset<Relation> merges = spark
			.read()
			.load(workingPath + "/" + testActionSetId + "/publication_mergerel")
			.as(Encoders.bean(Relation.class));

		assertEquals(
			3, merges
				.filter("relclass == 'isMergedIn'")
				.map((MapFunction<Relation, String>) Relation::getTarget, Encoders.STRING())
				.distinct()
				.count());
		assertEquals(
			4, merges
				.filter("source == '50|doi_dedup___::b3aec7985136e36827176aaa1dd5082d'")
				.count());

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

		assertEquals(3, roots.count());

		final Dataset<Publication> pubs = spark
			.read()
			.textFile(DedupUtility.createEntityPath(graphInputPath, "publication"))
			.map(asEntity(Publication.class), Encoders.bean(Publication.class));

		Publication root = roots
			.filter("id = '50|doi_dedup___::b3aec7985136e36827176aaa1dd5082d'")
			.first();
		assertNotNull(root);

		Publication crossref_duplicate = pubs
			.filter("id = '50|doi_________::b3aec7985136e36827176aaa1dd5082d'")
			.collectAsList()
			.get(0);

		assertEquals(crossref_duplicate.getDateofacceptance().getValue(), root.getDateofacceptance().getValue());
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
		assertTrue(rootPids.contains("10.1109/jstqe.2023.9999999"));

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

	private static String classPathResourceAsString(String path) throws IOException {
		return IOUtils
			.toString(
				SparkPublicationRootsTest2.class
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
