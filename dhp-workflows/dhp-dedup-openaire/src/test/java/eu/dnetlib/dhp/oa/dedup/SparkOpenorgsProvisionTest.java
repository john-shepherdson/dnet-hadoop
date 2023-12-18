
package eu.dnetlib.dhp.oa.dedup;

import static java.nio.file.Files.createTempDirectory;

import static org.apache.spark.sql.functions.col;
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

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SparkOpenorgsProvisionTest implements Serializable {

	@Mock(serializable = true)
	ISLookUpService isLookUpService;

	private static SparkSession spark;

	private static String testGraphBasePath;
	private static String testOutputBasePath;
	private static String testDedupGraphBasePath;
	private static String testConsistencyGraphBasePath;
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
		testConsistencyGraphBasePath = createTempDirectory(SparkOpenorgsProvisionTest.class.getSimpleName() + "-")
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
	}

	@AfterAll
	public static void finalCleanUp() throws IOException {
		FileUtils.deleteDirectory(new File(testOutputBasePath));
		FileUtils.deleteDirectory(new File(testDedupGraphBasePath));
		FileUtils.deleteDirectory(new File(testConsistencyGraphBasePath));
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

		Dataset<Row> organizations = spark.read().json(testDedupGraphBasePath + "/organization");

		Dataset<Row> mergedOrgs = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/organization_mergerel")
			.where("relClass=='merges'")
			.select("target")
			.distinct();

		assertEquals(80, organizations.count());

		Dataset<Row> deletedOrgs = organizations
			.filter("dataInfo.deletedbyinference = TRUE");

		assertEquals(mergedOrgs.count(), deletedOrgs.count());
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

		final Dataset<Row> outputRels = spark.read().text(testDedupGraphBasePath + "/relation");

		assertEquals(2382, outputRels.count());
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
					"-i", testDedupGraphBasePath, "-w", testOutputBasePath, "-o", testConsistencyGraphBasePath
				});

		new SparkPropagateRelation(parser, spark).run(isLookUpService);

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

		final Dataset<Row> mergedIds = mergeRels
			.where("relClass == 'merges'")
			.select(col("target").as("id"))
			.distinct();

		Dataset<Row> toUpdateRels = inputRels
			.as("rel")
			.join(mergedIds.as("s"), col("rel.source").equalTo(col("s.id")), "left_outer")
			.join(mergedIds.as("t"), col("rel.target").equalTo(col("t.id")), "left_outer")
			.filter("s.id IS NOT NULL OR t.id IS NOT NULL")
			.distinct();

		Dataset<Row> updatedRels = inputRels
			.select("source", "target", "relClass")
			.except(outputRels.select("source", "target", "relClass"));

		assertEquals(toUpdateRels.count(), updatedRels.count());
		assertEquals(140, outputRels.count());
	}
}
