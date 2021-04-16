
package eu.dnetlib.dhp.oa.dedup;

import static java.nio.file.Files.createTempDirectory;

import static org.apache.spark.sql.functions.count;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.lenient;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.util.MapDocumentUtil;
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
	private static final String testActionSetId = "test-orchestrator";

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

		FileUtils.deleteDirectory(new File(testOutputBasePath));
		FileUtils.deleteDirectory(new File(testDedupGraphBasePath));

		final SparkConf conf = new SparkConf();
		conf.set("spark.sql.shuffle.partitions", "200");
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
			.thenReturn(
				IOUtils
					.toString(
						SparkDedupTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/profiles/mock_orchestrator.xml")));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("organization")))
			.thenReturn(
				IOUtils
					.toString(
						SparkDedupTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json")));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("publication")))
			.thenReturn(
				IOUtils
					.toString(
						SparkDedupTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/conf/pub.curr.conf.json")));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("software")))
			.thenReturn(
				IOUtils
					.toString(
						SparkDedupTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/conf/sw.curr.conf.json")));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("dataset")))
			.thenReturn(
				IOUtils
					.toString(
						SparkDedupTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/conf/ds.curr.conf.json")));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("otherresearchproduct")))
			.thenReturn(
				IOUtils
					.toString(
						SparkDedupTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/conf/orp.curr.conf.json")));
	}

	@Test
	@Order(1)
	public void createSimRelsTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateSimRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/createSimRels_parameters.json")));

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

		assertEquals(3082, orgs_simrel);
		assertEquals(7036, pubs_simrel);
		assertEquals(344, sw_simrel);
		assertEquals(442, ds_simrel);
		assertEquals(6750, orp_simrel);
	}

	@Test
	@Order(2)
	public void cutMergeRelsTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateMergeRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json")));

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
					"3"
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
	public void createMergeRelsTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateMergeRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json")));

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

		new SparkCreateMergeRels(parser, spark).run(isLookUpService);

		long orgs_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/organization_mergerel")
			.count();
		long pubs_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/publication_mergerel")
			.count();
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

		assertEquals(1272, orgs_mergerel);
		assertEquals(1438, pubs_mergerel);
		assertEquals(288, sw_mergerel);
		assertEquals(472, ds_mergerel);
		assertEquals(718, orp_mergerel);

	}

	@Test
	@Order(4)
	public void createDedupRecordTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCreateDedupRecord.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/createDedupRecord_parameters.json")));
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

		long orgs_deduprecord = jsc
			.textFile(testOutputBasePath + "/" + testActionSetId + "/organization_deduprecord")
			.count();
		long pubs_deduprecord = jsc
			.textFile(testOutputBasePath + "/" + testActionSetId + "/publication_deduprecord")
			.count();
		long sw_deduprecord = jsc
			.textFile(testOutputBasePath + "/" + testActionSetId + "/software_deduprecord")
			.count();
		long ds_deduprecord = jsc.textFile(testOutputBasePath + "/" + testActionSetId + "/dataset_deduprecord").count();
		long orp_deduprecord = jsc
			.textFile(
				testOutputBasePath + "/" + testActionSetId + "/otherresearchproduct_deduprecord")
			.count();

		assertEquals(85, orgs_deduprecord);
		assertEquals(65, pubs_deduprecord);
		assertEquals(51, sw_deduprecord);
		assertEquals(97, ds_deduprecord);
		assertEquals(89, orp_deduprecord);
	}

	@Test
	@Order(5)
	public void updateEntityTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkUpdateEntity.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/updateEntity_parameters.json")));
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

		assertEquals(896, publications);
		assertEquals(838, organizations);
		assertEquals(100, projects);
		assertEquals(100, datasource);
		assertEquals(200, softwares);
		assertEquals(389, dataset);
		assertEquals(517, otherresearchproduct);

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
	public void propagateRelationTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkPropagateRelation.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/propagateRelation_parameters.json")));
		parser
			.parseArgument(
				new String[] {
					"-i", testGraphBasePath, "-w", testOutputBasePath, "-o", testDedupGraphBasePath
				});

		new SparkPropagateRelation(parser, spark).run(isLookUpService);

		long relations = jsc.textFile(testDedupGraphBasePath + "/relation").count();

		assertEquals(4718, relations);

		// check deletedbyinference
		final Dataset<Relation> mergeRels = spark
			.read()
			.load(DedupUtility.createMergeRelPath(testOutputBasePath, "*", "*"))
			.as(Encoders.bean(Relation.class));
		final JavaPairRDD<String, String> mergedIds = mergeRels
			.where("relClass == 'merges'")
			.select(mergeRels.col("target"))
			.distinct()
			.toJavaRDD()
			.mapToPair(
				(PairFunction<Row, String, String>) r -> new Tuple2<String, String>(r.getString(0), "d"));

		JavaRDD<String> toCheck = jsc
			.textFile(testDedupGraphBasePath + "/relation")
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

	@Test
	@Order(7)
	public void testRelations() throws Exception {
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
	}

	public boolean isDeletedByInference(String s) {
		return s.contains("\"deletedbyinference\":true");
	}
}
