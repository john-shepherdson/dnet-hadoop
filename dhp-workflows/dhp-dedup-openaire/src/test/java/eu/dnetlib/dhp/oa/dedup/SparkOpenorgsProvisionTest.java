
package eu.dnetlib.dhp.oa.dedup;

import static java.nio.file.Files.createTempDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.lenient;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.file.Paths;

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

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.util.MapDocumentUtil;
import scala.Tuple2;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SparkOpenorgsProvisionTest implements Serializable {

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
			.get(SparkOpenorgsProvisionTest.class.getResource("/eu/dnetlib/dhp/dedup/openorgs/provision").toURI())
			.toFile()
			.getAbsolutePath();
		testOutputBasePath = createTempDirectory(SparkOpenorgsProvisionTest.class.getSimpleName() + "-")
			.toAbsolutePath()
			.toString();
		testDedupGraphBasePath = createTempDirectory(SparkOpenorgsProvisionTest.class.getSimpleName() + "-")
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
								"/eu/dnetlib/dhp/dedup/profiles/mock_orchestrator_openorgs.xml")));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("organization")))
			.thenReturn(
				IOUtils
					.toString(
						SparkDedupTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json")));
	}

	@Test
	@Order(1)
	void copyOpenorgsMergeRelTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCopyOpenorgsMergeRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/copyOpenorgsMergeRels_parameters.json")));
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

		new SparkCopyOpenorgsMergeRels(parser, spark).run(isLookUpService);

		long orgs_mergerel = spark
			.read()
			.load(DedupUtility.createMergeRelPath(testOutputBasePath, testActionSetId, "organization"))
			.count();

		assertEquals(140, orgs_mergerel);
	}

	@Test
	@Order(2)
	void createOrgsDedupRecordTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCopyOpenorgsMergeRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/copyOpenorgs_parameters.json")));
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

		new SparkCreateOrgsDedupRecord(parser, spark).run(isLookUpService);

		long orgs_deduprecord = spark
			.read()
			.json(DedupUtility.createDedupRecordPath(testOutputBasePath, testActionSetId, "organization"))
			.count();

		assertEquals(10, orgs_deduprecord);
	}

	@Test
	@Order(3)
	void updateEntityTest() throws Exception {

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

		long mergedOrgs = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/organization_mergerel")
			.as(Encoders.bean(Relation.class))
			.where("relClass=='merges'")
			.javaRDD()
			.map(Relation::getTarget)
			.distinct()
			.count();

		assertEquals(80, organizations);

		long deletedOrgs = jsc
			.textFile(testDedupGraphBasePath + "/organization")
			.filter(this::isDeletedByInference)
			.count();

		assertEquals(mergedOrgs, deletedOrgs);
	}

	@Test
	@Order(4)
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

		final JavaRDD<String> rels = jsc.textFile(testDedupGraphBasePath + "/relation");

		assertEquals(2382, rels.count());

	}

	@Test
	@Order(5)
	void propagateRelationsTest() throws Exception {
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

		assertEquals(4896, relations);

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

	@AfterAll
	public static void finalCleanUp() throws IOException {
		FileUtils.deleteDirectory(new File(testOutputBasePath));
		FileUtils.deleteDirectory(new File(testDedupGraphBasePath));
	}

	public boolean isDeletedByInference(String s) {
		return s.contains("\"deletedbyinference\":true");
	}

}
