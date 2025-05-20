package eu.dnetlib.dhp.oa.dedup;

import static java.nio.file.Files.createTempDirectory;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.lenient;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SparkOpenorgsDedupTest implements Serializable {

	private static final String dbUrl = "jdbc:h2:mem:openorgs_test;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false";
	private static final String dbUser = "sa";
	private static final String dedupEventsTable = "tmp_dedup_events";
	private static final String parentChildTable = "tmp_parent_child";
	private static final String dbPwd = "";

	@Mock(serializable = true)
	ISLookUpService isLookUpService;

	protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
			.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	private static SparkSession spark;
	private static JavaSparkContext jsc;

	private static String testGraphBasePath;
	private static String testOutputBasePath;
	private static String testDedupGraphBasePath;
	private static final String testActionSetId = "test-orchestrator-openorgs";

	@BeforeAll
	public static void cleanUp() throws IOException, URISyntaxException {

		testGraphBasePath = Paths
				.get(SparkOpenorgsDedupTest.class.getResource("/eu/dnetlib/dhp/dedup/openorgs/dedup").toURI())
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
										SparkOpenorgsDedupTest.class
												.getResourceAsStream(
														"/eu/dnetlib/dhp/dedup/profiles/mock_orchestrator_openorgs.xml")));

		lenient()
				.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("organization")))
				.thenReturn(
						IOUtils
								.toString(
										SparkOpenorgsDedupTest.class
												.getResourceAsStream(
														"/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json")));
	}

	@Test
	@Order(1)
	void createSimRelsTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils
						.toString(
								SparkCreateSimRels.class
										.getResourceAsStream(
												"/eu/dnetlib/dhp/oa/dedup/createSimRels_parameters.json")));

		parser
				.parseArgument(
						new String[]{
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

		System.out.println("orgs_simrel = " + orgs_simrel);
		assertEquals(95, orgs_simrel);
	}

	@Test
	@Order(2)
	void copyOpenorgsSimRels() throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils
						.toString(
								SparkCopyOpenorgsSimRels.class
										.getResourceAsStream(
												"/eu/dnetlib/dhp/oa/dedup/copyOpenorgsMergeRels_parameters.json")));
		parser
				.parseArgument(
						new String[]{
								"-i", testGraphBasePath,
								"-asi", testActionSetId,
								"-w", testOutputBasePath,
								"-la", "lookupurl",
								"-np", "50"
						});

		new SparkCopyOpenorgsSimRels(parser, spark).run(isLookUpService);

		long orgs_simrel = spark
				.read()
				.load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "organization"))
				.count();

		System.out.println("orgs_simrel = " + orgs_simrel);
		assertEquals(131, orgs_simrel);
	}

	@Test
	@Order(3)
	void createMergeRelsTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils
						.toString(
								SparkCreateMergeRels.class
										.getResourceAsStream(
												"/eu/dnetlib/dhp/oa/dedup/createCC_parameters.json")));

		parser
				.parseArgument(
						new String[]{
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
		assertEquals(132, orgs_mergerel);

		// verify that a DiffRel is in the mergerels (to be sure that the job supposed to remove them has something to
		// do)
		List<String> diffRels = jsc
				.textFile(DedupUtility.createEntityPath(testGraphBasePath, "relation"))
				.map(s -> OBJECT_MAPPER.readValue(s, Relation.class))
				.filter(r -> r.getRelClass().equals("isDifferentFrom"))
				.map(r -> r.getTarget())
				.collect();
		assertEquals(18, diffRels.size());

		List<String> mergeRels = spark
				.read()
				.load(testOutputBasePath + "/" + testActionSetId + "/organization_mergerel")
				.as(Encoders.bean(Relation.class))
				.toJavaRDD()
				.map(r -> r.getTarget())
				.collect();
		assertFalse(Collections.disjoint(mergeRels, diffRels));

	}

	@Test
	@Order(4)
	void refineMergeRelsTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils
						.toString(
								SparkRefineMergeRels.class
										.getResourceAsStream(
												"/eu/dnetlib/dhp/oa/dedup/refineMergeRels_parameters.json")));

		parser
				.parseArgument(
						new String[]{
								"-i",
								testGraphBasePath,
								"-asi",
								testActionSetId,
								"-la",
								"lookupurl",
								"-w",
								testOutputBasePath
						});

		new SparkRefineMergeRels(parser, spark).run(isLookUpService);

		long orgs_mergerel = spark
				.read()
				.load(testOutputBasePath + "/" + testActionSetId + "/organization_mergerel")
				.count();
		assertEquals(130, orgs_mergerel);

	}

	@Test
	@Order(5)
	void prepareOrgRelsTest() throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils
						.toString(
								SparkPrepareOrgRels.class
										.getResourceAsStream(
												"/eu/dnetlib/dhp/oa/dedup/prepareOrgRels_parameters.json")));
		parser
				.parseArgument(
						new String[]{
								"-i",
								testGraphBasePath,
								"-asi",
								testActionSetId,
								"-la",
								"lookupurl",
								"-w",
								testOutputBasePath,
								"-du",
								dbUrl,
								"-dusr",
								dbUser,
								"-tde",
								dedupEventsTable,
								"-tpc",
								parentChildTable,
								"-dpwd",
								dbPwd
						});

		new SparkPrepareOrgRels(parser, spark).run(isLookUpService);

		final Properties connectionProperties = new Properties();
		connectionProperties.put("user", dbUser);
		connectionProperties.put("password", dbPwd);

		Connection connection = DriverManager.getConnection(dbUrl, connectionProperties);

		// verify the number of dedup events in the DB
		ResultSet resultSet = connection
				.prepareStatement("SELECT COUNT(*) as total_rels FROM " + dedupEventsTable)
				.executeQuery();
		if (resultSet.next()) {
			int total_rels = resultSet.getInt("total_rels");
			assertEquals(35, total_rels);
		} else
			fail("No result in the sql DB");
		resultSet.close();

		// verify the number of organizations with duplicates
		ResultSet resultSet2 = connection
				.prepareStatement("SELECT COUNT(DISTINCT(local_id)) as total_orgs FROM " + dedupEventsTable)
				.executeQuery();
		if (resultSet2.next()) {
			int total_orgs = resultSet2.getInt("total_orgs");
			assertEquals(8, total_orgs);
		} else
			fail("No result in the sql DB");
		resultSet2.close();

		// collect the DiffRels from the input graph
		List<String> diffRels = jsc
				.textFile(DedupUtility.createEntityPath(testGraphBasePath, "relation"))
				.map(s -> OBJECT_MAPPER.readValue(s, Relation.class))
				.filter(r -> r.getRelClass().equals(ModelConstants.IS_DIFFERENT_FROM))
				.map(r -> r.getSource() + "@@@" + r.getTarget())
				.collect();

		// collect the ParentChildRels from the input graph and create families: <id, familyId>
		Dataset<Row> families = OpenorgsUtility.createFamilies(
				spark,
				DedupUtility.createEntityPath(testGraphBasePath, "relation"),
				ModelConstants.IS_PARENT_OF);
		List<Row> rows = new ArrayList<>();
		ResultSet resultSet3 = connection
				.prepareStatement("SELECT local_id, oa_original_id FROM " + dedupEventsTable)
				.executeQuery();
		while (resultSet3.next()) {
			String source = OafMapperUtils.createOpenaireId("organization", resultSet3.getString("local_id"), true);
			String target = OafMapperUtils
					.createOpenaireId("organization", resultSet3.getString("oa_original_id"), true);
			rows.add(RowFactory.create(source, target));
		}
		resultSet3.close();
		Dataset<Row> duplicateSuggestions = spark.createDataFrame(
				rows,
				new StructType()
						.add("source", DataTypes.StringType, false)
						.add("target", DataTypes.StringType, false)
		);

		// verify that the DiffRels are not in the DB
		List<String> dbRels = duplicateSuggestions.toJavaRDD().map(r -> r.get(0) + "@@@" + r.get(1)).collect();
		assertTrue(Collections.disjoint(dbRels, diffRels));

		// verify that the suggestions are not in same family
		long family_conflicts_num = duplicateSuggestions
				.join(families, duplicateSuggestions.col("target").equalTo(families.col("id")))
				.groupBy(duplicateSuggestions.col("source"))
				.agg(
						count("groupId").alias("total"),
						countDistinct("groupId").alias("distinct")
				)
				.filter(col("total").gt(col("distinct")))
				.count();
		assertEquals(0, family_conflicts_num);

		// verify the number of parent child suggestions in the DB
		ResultSet resultSet4 = connection
				.prepareStatement("SELECT COUNT(*) as total_rels FROM " + parentChildTable)
				.executeQuery();
		if (resultSet4.next()) {
			int total_rels = resultSet4.getInt("total_rels");
			assertEquals(4, total_rels);
		} else
			fail("No result in the sql DB");
		resultSet.close();

		connection.close();
	}

	@Test
	@Order(6)
	void prepareNewOrgsTest() throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils
						.toString(
								SparkPrepareNewOrgs.class
										.getResourceAsStream(
												"/eu/dnetlib/dhp/oa/dedup/prepareNewOrgs_parameters.json")));
		parser
				.parseArgument(
						new String[]{
								"-i",
								testGraphBasePath,
								"-asi",
								testActionSetId,
								"-la",
								"lookupurl",
								"-w",
								testOutputBasePath,
								"-du",
								dbUrl,
								"-dusr",
								dbUser,
								"-tde",
								dedupEventsTable,
								"-dpwd",
								dbPwd
						});

		new SparkPrepareNewOrgs(parser, spark).run(isLookUpService);

		final Properties connectionProperties = new Properties();
		connectionProperties.put("user", dbUser);
		connectionProperties.put("password", dbPwd);

		long orgs_in_diffrel = jsc
				.textFile(DedupUtility.createEntityPath(testGraphBasePath, "relation"))
				.map(s -> OBJECT_MAPPER.readValue(s, Relation.class))
				.filter(r -> r.getRelClass().equals("isDifferentFrom"))
				.map(r -> r.getTarget())
				.distinct()
				.count();

		Connection connection = DriverManager.getConnection(dbUrl, connectionProperties);

		jsc
				.textFile(DedupUtility.createEntityPath(testGraphBasePath, "relation"))
				.map(s -> OBJECT_MAPPER.readValue(s, Relation.class))
				.filter(r -> r.getRelClass().equals("isDifferentFrom"))
				.map(r -> r.getTarget())
				.distinct()
				.foreach(s -> System.out.println("difforgs = " + s));
		ResultSet resultSet0 = connection
				.prepareStatement("SELECT oa_original_id FROM " + dedupEventsTable + " WHERE local_id = ''")
				.executeQuery();
		while (resultSet0.next())
			System.out
					.println(
							"dborgs = " + OafMapperUtils.createOpenaireId(20, resultSet0.getString("oa_original_id"), true));
		resultSet0.close();

		ResultSet resultSet = connection
				.prepareStatement("SELECT COUNT(*) as total_new_orgs FROM " + dedupEventsTable + " WHERE local_id = ''")
				.executeQuery();
		if (resultSet.next()) {
			int total_new_orgs = resultSet.getInt("total_new_orgs");
			assertEquals(orgs_in_diffrel, total_new_orgs);
		} else
			fail("No result in the sql DB");
		resultSet.close();
	}

	@AfterAll
	public static void finalCleanUp() throws IOException {
		FileUtils.deleteDirectory(new File(testOutputBasePath));
		FileUtils.deleteDirectory(new File(testDedupGraphBasePath));
	}

}