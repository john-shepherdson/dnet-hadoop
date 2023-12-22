
package eu.dnetlib.dhp.oa.dedup;

import static java.nio.file.Files.createTempDirectory;

import static org.apache.spark.sql.functions.count;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.lenient;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
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
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.schema.sx.OafUtils;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;

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
	private static String testConsistencyGraphBasePath;

	private static final String testActionSetId = "test-orchestrator";
	private static String whitelistPath;
	private static List<String> whiteList;

	private static String WHITELIST_SEPARATOR = "####";

	@BeforeAll
	public static void cleanUp() throws IOException, URISyntaxException {

		testGraphBasePath = Paths
			.get(SparkDedupTest.class.getResource("/eu/dnetlib/dhp/dedup/entities").toURI())
			.toFile()
			.getAbsolutePath();

		testOutputBasePath = createTempDirectory(SparkDedupTest.class.getSimpleName() + "-")
			.toAbsolutePath()
			.toString();

		testDedupGraphBasePath = createTempDirectory(SparkDedupTest.class.getSimpleName() + "-")
			.toAbsolutePath()
			.toString();

		testConsistencyGraphBasePath = createTempDirectory(SparkDedupTest.class.getSimpleName() + "-")
			.toAbsolutePath()
			.toString();

		whitelistPath = Paths
			.get(SparkDedupTest.class.getResource("/eu/dnetlib/dhp/dedup/whitelist.simrels.txt").toURI())
			.toFile()
			.getAbsolutePath();
		whiteList = IOUtils.readLines(new FileReader(whitelistPath));

		FileUtils.deleteDirectory(new File(testOutputBasePath));
		FileUtils.deleteDirectory(new File(testDedupGraphBasePath));

		final SparkConf conf = new SparkConf();
		conf.set("spark.sql.shuffle.partitions", "200");
		conf.set("spark.sql.warehouse.dir", testOutputBasePath + "/spark-warehouse");
		spark = SparkSession
			.builder()
			.appName(SparkDedupTest.class.getSimpleName())
			.master("local[*]")
			.config(conf)
			.getOrCreate();

		jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

	}

	@BeforeEach
	public void setUp() throws IOException, ISLookUpException {

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains(testActionSetId)))
			.thenReturn(classPathResourceAsString("/eu/dnetlib/dhp/dedup/profiles/mock_orchestrator.xml"));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("organization")))
			.thenReturn(classPathResourceAsString("/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json"));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("publication")))
			.thenReturn(classPathResourceAsString("/eu/dnetlib/dhp/dedup/conf/pub.curr.conf.json"));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("software")))
			.thenReturn(classPathResourceAsString("/eu/dnetlib/dhp/dedup/conf/sw.curr.conf.json"));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("dataset")))
			.thenReturn(classPathResourceAsString("/eu/dnetlib/dhp/dedup/conf/ds.curr.conf.json"));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("otherresearchproduct")))
			.thenReturn(classPathResourceAsString("/eu/dnetlib/dhp/dedup/conf/orp.curr.conf.json"));
	}

	@Test
	@Order(1)
	void createSimRelsTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			classPathResourceAsString("/eu/dnetlib/dhp/oa/dedup/createSimRels_parameters.json"));

		parser
			.parseArgument(
				new String[] {
					"-i", testGraphBasePath,
					"-asi", testActionSetId,
					"-la", "lookupurl",
					"-w", testOutputBasePath,
					"-np", "50"
				});

		new SparkCreateSimRels(parser, spark).run(isLookUpService);

		long orgs_simrel = spark
			.read()
			.load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "organization"))
			.count();

		long pubs_simrel = spark
			.read()
			.load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "publication"))
			.count();

		long sw_simrel = spark
			.read()
			.load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "software"))
			.count();

		long ds_simrel = spark
			.read()
			.load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "dataset"))
			.count();

		long orp_simrel = spark
			.read()
			.load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "otherresearchproduct"))
			.count();

		System.out.println("orgs_simrel = " + orgs_simrel);
		System.out.println("pubs_simrel = " + pubs_simrel);
		System.out.println("sw_simrel = " + sw_simrel);
		System.out.println("ds_simrel = " + ds_simrel);
		System.out.println("orp_simrel = " + orp_simrel);

		assertEquals(751, orgs_simrel);
		assertEquals(546, pubs_simrel);
		assertEquals(113, sw_simrel);
		assertEquals(148, ds_simrel);
		assertEquals(280, orp_simrel);

	}

	@Test
	@Order(2)
	void whitelistSimRelsTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			classPathResourceAsString("/eu/dnetlib/dhp/oa/dedup/whitelistSimRels_parameters.json"));

		parser
			.parseArgument(
				new String[] {
					"-i", testGraphBasePath,
					"-asi", testActionSetId,
					"-la", "lookupurl",
					"-w", testOutputBasePath,
					"-np", "50",
					"-wl", whitelistPath
				});

		new SparkWhitelistSimRels(parser, spark).run(isLookUpService);

		long orgs_simrel = spark
			.read()
			.load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "organization"))
			.count();

		long pubs_simrel = spark
			.read()
			.load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "publication"))
			.count();

		long ds_simrel = spark
			.read()
			.load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "dataset"))
			.count();

		long orp_simrel = spark
			.read()
			.load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "otherresearchproduct"))
			.count();

		// entities simrels supposed to be equal to the number of previous step (no rels in whitelist)
		assertEquals(751, orgs_simrel);
		assertEquals(546, pubs_simrel);
		assertEquals(148, ds_simrel);
		assertEquals(280, orp_simrel);
//		System.out.println("orgs_simrel = " + orgs_simrel);
//		System.out.println("pubs_simrel = " + pubs_simrel);
//		System.out.println("ds_simrel = " + ds_simrel);
//		System.out.println("orp_simrel = " + orp_simrel);

		// entities simrels to be different from the number of previous step (new simrels in the whitelist)
		Dataset<Row> sw_simrel = spark
			.read()
			.load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "software"));

		// check if the first relation in the whitelist exists
		assertTrue(
			sw_simrel
				.as(Encoders.bean(Relation.class))
				.toJavaRDD()
				.filter(
					rel -> rel.getSource().equalsIgnoreCase(whiteList.get(0).split(WHITELIST_SEPARATOR)[0])
						&& rel.getTarget().equalsIgnoreCase(whiteList.get(0).split(WHITELIST_SEPARATOR)[1]))
				.count() > 0);
		// check if the second relation in the whitelist exists
		assertTrue(
			sw_simrel
				.as(Encoders.bean(Relation.class))
				.toJavaRDD()
				.filter(
					rel -> rel.getSource().equalsIgnoreCase(whiteList.get(1).split(WHITELIST_SEPARATOR)[0])
						&& rel.getTarget().equalsIgnoreCase(whiteList.get(1).split(WHITELIST_SEPARATOR)[1]))
				.count() > 0);

		assertEquals(115, sw_simrel.count());
//		System.out.println("sw_simrel = " + sw_simrel.count());

	}

	@Test
	@Order(3)
	void cutMergeRelsTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			classPathResourceAsString("/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json"));

		parser
			.parseArgument(
				new String[] {
					"-i",
					testGraphBasePath,
					"-asi",
					testActionSetId,
					"-la",
					"lookupurl",
					"-w",
					testOutputBasePath,
					"-cc",
					"3",
					"-h",
					""
				});

		new SparkCreateMergeRels(parser, spark).run(isLookUpService);

		long orgs_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/organization_mergerel")
			.as(Encoders.bean(Relation.class))
			.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase("merges"))
			.groupBy("source")
			.agg(count("target").alias("cnt"))
			.select("source", "cnt")
			.where("cnt > 3")
			.count();

		long pubs_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/publication_mergerel")
			.as(Encoders.bean(Relation.class))
			.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase("merges"))
			.groupBy("source")
			.agg(count("target").alias("cnt"))
			.select("source", "cnt")
			.where("cnt > 3")
			.count();
		long sw_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/software_mergerel")
			.as(Encoders.bean(Relation.class))
			.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase("merges"))
			.groupBy("source")
			.agg(count("target").alias("cnt"))
			.select("source", "cnt")
			.where("cnt > 3")
			.count();

		long ds_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/dataset_mergerel")
			.as(Encoders.bean(Relation.class))
			.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase("merges"))
			.groupBy("source")
			.agg(count("target").alias("cnt"))
			.select("source", "cnt")
			.where("cnt > 3")
			.count();

		long orp_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/otherresearchproduct_mergerel")
			.as(Encoders.bean(Relation.class))
			.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase("merges"))
			.groupBy("source")
			.agg(count("target").alias("cnt"))
			.select("source", "cnt")
			.where("cnt > 3")
			.count();

		assertEquals(0, orgs_mergerel);
		assertEquals(0, pubs_mergerel);
		assertEquals(0, sw_mergerel);
		assertEquals(0, ds_mergerel);
		assertEquals(0, orp_mergerel);

		FileUtils.deleteDirectory(new File(testOutputBasePath + "/" + testActionSetId + "/organization_mergerel"));
		FileUtils.deleteDirectory(new File(testOutputBasePath + "/" + testActionSetId + "/publication_mergerel"));
		FileUtils.deleteDirectory(new File(testOutputBasePath + "/" + testActionSetId + "/software_mergerel"));
		FileUtils.deleteDirectory(new File(testOutputBasePath + "/" + testActionSetId + "/dataset_mergerel"));
		FileUtils
			.deleteDirectory(new File(testOutputBasePath + "/" + testActionSetId + "/otherresearchproduct_mergerel"));
	}

	@Test
	@Order(3)
	void createMergeRelsWithPivotHistoryTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			classPathResourceAsString("/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json"));

		spark.sql("CREATE DATABASE IF NOT EXISTS pivot_history_test");
		ModelSupport.oafTypes.keySet().forEach(entityType -> {
			try {
				spark
					.read()
					.json(
						Paths
							.get(SparkDedupTest.class.getResource("/eu/dnetlib/dhp/dedup/pivot_history").toURI())
							.toFile()
							.getAbsolutePath())
					.write()
					.mode("overwrite")
					.saveAsTable("pivot_history_test." + entityType);
			} catch (URISyntaxException e) {
				throw new RuntimeException(e);
			}
		});

		parser
			.parseArgument(
				new String[] {
					"-i",
					testGraphBasePath,
					"-asi",
					testActionSetId,
					"-la",
					"lookupurl",
					"-w",
					testOutputBasePath,
					"-h",
					"",
					"-pivotHistoryDatabase",
					"pivot_history_test"

				});

		new SparkCreateMergeRels(parser, spark).run(isLookUpService);

		long orgs_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/organization_mergerel")
			.count();
		final Dataset<Relation> pubs = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/publication_mergerel")
			.as(Encoders.bean(Relation.class));
		long sw_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/software_mergerel")
			.count();
		long ds_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/dataset_mergerel")
			.count();

		long orp_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/otherresearchproduct_mergerel")
			.count();

		final List<Relation> merges = pubs
			.filter("source == '50|arXiv_dedup_::c93aeb433eb90ed7a86e29be00791b7c'")
			.collectAsList();
		assertEquals(3, merges.size());
		Set<String> dups = Sets
			.newHashSet(
				"50|doi_________::3b1d0d8e8f930826665df9d6b82fbb73",
				"50|doi_________::d5021b53204e4fdeab6ff5d5bc468032",
				"50|arXiv_______::c93aeb433eb90ed7a86e29be00791b7c");
		merges.forEach(r -> {
			assertEquals(ModelConstants.RESULT_RESULT, r.getRelType());
			assertEquals(ModelConstants.DEDUP, r.getSubRelType());
			assertEquals(ModelConstants.MERGES, r.getRelClass());
			assertTrue(dups.contains(r.getTarget()));
		});

		final List<Relation> mergedIn = pubs
			.filter("target == '50|arXiv_dedup_::c93aeb433eb90ed7a86e29be00791b7c'")
			.collectAsList();
		assertEquals(3, mergedIn.size());
		mergedIn.forEach(r -> {
			assertEquals(ModelConstants.RESULT_RESULT, r.getRelType());
			assertEquals(ModelConstants.DEDUP, r.getSubRelType());
			assertEquals(ModelConstants.IS_MERGED_IN, r.getRelClass());
			assertTrue(dups.contains(r.getSource()));
		});

		assertEquals(1268, orgs_mergerel);
		assertEquals(1112, pubs.count());
		assertEquals(292, sw_mergerel);
		assertEquals(476, ds_mergerel);
		assertEquals(742, orp_mergerel);
//		System.out.println("orgs_mergerel = " + orgs_mergerel);
//		System.out.println("pubs_mergerel = " + pubs_mergerel);
//		System.out.println("sw_mergerel = " + sw_mergerel);
//		System.out.println("ds_mergerel = " + ds_mergerel);
//		System.out.println("orp_mergerel = " + orp_mergerel);

	}

	@Test
	@Order(4)
	void createMergeRelsTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			classPathResourceAsString("/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json"));

		parser
			.parseArgument(
				new String[] {
					"-i",
					testGraphBasePath,
					"-asi",
					testActionSetId,
					"-la",
					"lookupurl",
					"-w",
					testOutputBasePath,
					"-h",
					""
				});

		new SparkCreateMergeRels(parser, spark).run(isLookUpService);

		long orgs_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/organization_mergerel")
			.count();
		final Dataset<Relation> pubs = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/publication_mergerel")
			.as(Encoders.bean(Relation.class));
		long sw_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/software_mergerel")
			.count();
		long ds_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/dataset_mergerel")
			.count();

		long orp_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/otherresearchproduct_mergerel")
			.count();

		final List<Relation> merges = pubs
			.filter("source == '50|doi_dedup___::d5021b53204e4fdeab6ff5d5bc468032'")
			.collectAsList();
		assertEquals(3, merges.size());
		Set<String> dups = Sets
			.newHashSet(
				"50|doi_________::3b1d0d8e8f930826665df9d6b82fbb73",
				"50|doi_________::d5021b53204e4fdeab6ff5d5bc468032",
				"50|arXiv_______::c93aeb433eb90ed7a86e29be00791b7c");
		merges.forEach(r -> {
			assertEquals(ModelConstants.RESULT_RESULT, r.getRelType());
			assertEquals(ModelConstants.DEDUP, r.getSubRelType());
			assertEquals(ModelConstants.MERGES, r.getRelClass());
			assertTrue(dups.contains(r.getTarget()));
		});

		final List<Relation> mergedIn = pubs
			.filter("target == '50|doi_dedup___::d5021b53204e4fdeab6ff5d5bc468032'")
			.collectAsList();
		assertEquals(3, mergedIn.size());
		mergedIn.forEach(r -> {
			assertEquals(ModelConstants.RESULT_RESULT, r.getRelType());
			assertEquals(ModelConstants.DEDUP, r.getSubRelType());
			assertEquals(ModelConstants.IS_MERGED_IN, r.getRelClass());
			assertTrue(dups.contains(r.getSource()));
		});

		assertEquals(1268, orgs_mergerel);
		assertEquals(1112, pubs.count());
		assertEquals(292, sw_mergerel);
		assertEquals(476, ds_mergerel);
		assertEquals(742, orp_mergerel);
//		System.out.println("orgs_mergerel = " + orgs_mergerel);
//		System.out.println("pubs_mergerel = " + pubs_mergerel);
//		System.out.println("sw_mergerel = " + sw_mergerel);
//		System.out.println("ds_mergerel = " + ds_mergerel);
//		System.out.println("orp_mergerel = " + orp_mergerel);

	}

	@Test
	@Order(5)
	void createDedupRecordTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			classPathResourceAsString("/eu/dnetlib/dhp/oa/dedup/createDedupRecord_parameters.json"));
		parser
			.parseArgument(
				new String[] {
					"-i",
					testGraphBasePath,
					"-asi",
					testActionSetId,
					"-la",
					"lookupurl",
					"-w",
					testOutputBasePath
				});

		new SparkCreateDedupRecord(parser, spark).run(isLookUpService);

		final ObjectMapper mapper = new ObjectMapper()
			.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		final Dataset<Publication> pubs = spark
			.read()
			.textFile(testOutputBasePath + "/" + testActionSetId + "/publication_deduprecord")
			.map(
				(MapFunction<String, Publication>) value -> mapper.readValue(value, Publication.class),
				Encoders.bean(Publication.class));
		long orgs_deduprecord = jsc
			.textFile(testOutputBasePath + "/" + testActionSetId + "/organization_deduprecord")
			.count();
		long sw_deduprecord = jsc
			.textFile(testOutputBasePath + "/" + testActionSetId + "/software_deduprecord")
			.count();
		long ds_deduprecord = jsc.textFile(testOutputBasePath + "/" + testActionSetId + "/dataset_deduprecord").count();
		long orp_deduprecord = jsc
			.textFile(
				testOutputBasePath + "/" + testActionSetId + "/otherresearchproduct_deduprecord")
			.count();

		assertEquals(86, orgs_deduprecord);
		assertEquals(91, pubs.count());
		assertEquals(47, sw_deduprecord);
		assertEquals(97, ds_deduprecord);
		assertEquals(92, orp_deduprecord);

		verifyRoot_1(mapper, pubs);

//		System.out.println("orgs_deduprecord = " + orgs_deduprecord);
//		System.out.println("pubs_deduprecord = " + pubs_deduprecord);
//		System.out.println("sw_deduprecord = " + sw_deduprecord);
//		System.out.println("ds_deduprecord = " + ds_deduprecord);
//		System.out.println("orp_deduprecord = " + orp_deduprecord);
	}

	private static void verifyRoot_1(ObjectMapper mapper, Dataset<Publication> pubs) {
		Publication root = pubs
			.filter("id = '50|doi_dedup___::d5021b53204e4fdeab6ff5d5bc468032'")
			.first();
		assertNotNull(root);

		final Dataset<String> publication = spark
			.read()
			.textFile(DedupUtility.createEntityPath(testGraphBasePath, "publication"));

		Publication crossref_duplicate = publication
			.map(
				(MapFunction<String, Publication>) value -> mapper.readValue(value, Publication.class),
				Encoders.bean(Publication.class))
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

	@Test
	@Order(6)
	void updateEntityTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			classPathResourceAsString("/eu/dnetlib/dhp/oa/dedup/updateEntity_parameters.json"));
		parser
			.parseArgument(
				new String[] {
					"-i", testGraphBasePath, "-w", testOutputBasePath, "-o", testDedupGraphBasePath
				});

		new SparkUpdateEntity(parser, spark).run(isLookUpService);

		long organizations = jsc.textFile(testDedupGraphBasePath + "/organization").count();
		long publications = jsc.textFile(testDedupGraphBasePath + "/publication").count();
		long projects = jsc.textFile(testDedupGraphBasePath + "/project").count();
		long datasource = jsc.textFile(testDedupGraphBasePath + "/datasource").count();
		long softwares = jsc.textFile(testDedupGraphBasePath + "/software").count();
		long dataset = jsc.textFile(testDedupGraphBasePath + "/dataset").count();
		long otherresearchproduct = jsc.textFile(testDedupGraphBasePath + "/otherresearchproduct").count();

		long mergedOrgs = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/organization_mergerel")
			.as(Encoders.bean(Relation.class))
			.where("relClass=='merges'")
			.javaRDD()
			.map(Relation::getTarget)
			.distinct()
			.count();

		long mergedPubs = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/publication_mergerel")
			.as(Encoders.bean(Relation.class))
			.where("relClass=='merges'")
			.javaRDD()
			.map(Relation::getTarget)
			.distinct()
			.count();

		long mergedSw = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/software_mergerel")
			.as(Encoders.bean(Relation.class))
			.where("relClass=='merges'")
			.javaRDD()
			.map(Relation::getTarget)
			.distinct()
			.count();

		long mergedDs = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/dataset_mergerel")
			.as(Encoders.bean(Relation.class))
			.where("relClass=='merges'")
			.javaRDD()
			.map(Relation::getTarget)
			.distinct()
			.count();

		long mergedOrp = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/otherresearchproduct_mergerel")
			.as(Encoders.bean(Relation.class))
			.where("relClass=='merges'")
			.javaRDD()
			.map(Relation::getTarget)
			.distinct()
			.count();

		assertEquals(925, publications);
		assertEquals(839, organizations);
		assertEquals(100, projects);
		assertEquals(100, datasource);
		assertEquals(196, softwares);
		assertEquals(389, dataset);
		assertEquals(520, otherresearchproduct);

//		System.out.println("publications = " + publications);
//		System.out.println("organizations = " + organizations);
//		System.out.println("projects = " + projects);
//		System.out.println("datasource = " + datasource);
//		System.out.println("software = " + softwares);
//		System.out.println("dataset = " + dataset);
//		System.out.println("otherresearchproduct = " + otherresearchproduct);

		long deletedOrgs = jsc
			.textFile(testDedupGraphBasePath + "/organization")
			.filter(this::isDeletedByInference)
			.count();

		long deletedPubs = jsc
			.textFile(testDedupGraphBasePath + "/publication")
			.filter(this::isDeletedByInference)
			.count();

		long deletedSw = jsc
			.textFile(testDedupGraphBasePath + "/software")
			.filter(this::isDeletedByInference)
			.count();

		long deletedDs = jsc
			.textFile(testDedupGraphBasePath + "/dataset")
			.filter(this::isDeletedByInference)
			.count();

		long deletedOrp = jsc
			.textFile(testDedupGraphBasePath + "/otherresearchproduct")
			.filter(this::isDeletedByInference)
			.count();

		assertEquals(mergedOrgs, deletedOrgs);
		assertEquals(mergedPubs, deletedPubs);
		assertEquals(mergedSw, deletedSw);
		assertEquals(mergedDs, deletedDs);
		assertEquals(mergedOrp, deletedOrp);
	}

	@Test
	@Order(6)
	void copyRelationsNoOpenorgsTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCopyRelationsNoOpenorgs.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/updateEntity_parameters.json")));
		parser
			.parseArgument(
				new String[] {
					"-i", testGraphBasePath, "-w", testOutputBasePath, "-o", testDedupGraphBasePath
				});

		new SparkCopyRelationsNoOpenorgs(parser, spark).run(isLookUpService);

		final Dataset<Row> outputRels = spark.read().text(testDedupGraphBasePath + "/relation");

		System.out.println(outputRels.count());
		// assertEquals(2382, outputRels.count());
	}

	@Test
	@Order(7)
	void propagateRelationTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			classPathResourceAsString("/eu/dnetlib/dhp/oa/dedup/propagateRelation_parameters.json"));
		parser
			.parseArgument(
				new String[] {
					"-i", testDedupGraphBasePath, "-w", testOutputBasePath, "-o", testConsistencyGraphBasePath
				});

		new SparkPropagateRelation(parser, spark).run(isLookUpService);

		long relations = jsc.textFile(testDedupGraphBasePath + "/relation").count();

//		assertEquals(4860, relations);
		System.out.println("relations = " + relations);

		// check deletedbyinference
		final Dataset<Relation> mergeRels = spark
			.read()
			.load(DedupUtility.createMergeRelPath(testOutputBasePath, "*", "*"))
			.as(Encoders.bean(Relation.class));

		Dataset<Row> inputRels = spark
			.read()
			.json(testDedupGraphBasePath + "/relation");

		Dataset<Row> outputRels = spark
			.read()
			.json(testConsistencyGraphBasePath + "/relation");

		assertEquals(
			0, outputRels
				.filter("dataInfo.deletedbyinference == true OR dataInfo.invisible == true")
				.count());

		assertEquals(
			5, outputRels
				.filter("relClass NOT IN ('merges', 'isMergedIn')")
				.count());

		assertEquals(5 + mergeRels.count(), outputRels.count());
	}

	@Test
	@Order(8)
	void testCleanedPropagatedRelations() throws Exception {
		Dataset<Row> df_before = spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(testDedupGraphBasePath + "/relation");

		Dataset<Row> df_after = spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(testConsistencyGraphBasePath + "/relation");

		assertNotEquals(df_before.count(), df_after.count());

		assertEquals(
			0, df_after
				.filter("dataInfo.deletedbyinference == true OR dataInfo.invisible == true")
				.count());

		assertEquals(
			5, df_after
				.filter("relClass NOT IN ('merges', 'isMergedIn')")
				.count());
	}

	@Test
	@Order(10)
	void testRelations() throws Exception {
		testUniqueness("/eu/dnetlib/dhp/dedup/test/relation_1.json", 12, 10);
		testUniqueness("/eu/dnetlib/dhp/dedup/test/relation_2.json", 10, 2);
	}

	private void testUniqueness(String path, int expected_total, int expected_unique) {
		Dataset<Relation> rel = spark
			.read()
			.textFile(getClass().getResource(path).getPath())
			.map(
				(MapFunction<String, Relation>) s -> new ObjectMapper().readValue(s, Relation.class),
				Encoders.bean(Relation.class));

		assertEquals(expected_total, rel.count());
		assertEquals(expected_unique, rel.distinct().count());
	}

	@AfterAll
	public static void finalCleanUp() throws IOException {
		FileUtils.deleteDirectory(new File(testOutputBasePath));
		FileUtils.deleteDirectory(new File(testDedupGraphBasePath));
		FileUtils.deleteDirectory(new File(testConsistencyGraphBasePath));
	}

	public boolean isDeletedByInference(String s) {
		return s.contains("\"deletedbyinference\":true");
	}

	private static String classPathResourceAsString(String path) throws IOException {
		return IOUtils
			.toString(
				SparkDedupTest.class
					.getResourceAsStream(path));
	}

}
