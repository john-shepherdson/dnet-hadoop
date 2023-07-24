
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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
public class SparkStatsTest implements Serializable {

	@Mock(serializable = true)
	ISLookUpService isLookUpService;

	private static SparkSession spark;
	private static JavaSparkContext jsc;

	private static String testGraphBasePath;
	private static String testOutputBasePath;
	private static final String testActionSetId = "test-orchestrator";

	@BeforeAll
	public static void beforeAll() throws IOException, URISyntaxException {

		testGraphBasePath = Paths
			.get(SparkDedupTest.class.getResource("/eu/dnetlib/dhp/dedup/entities").toURI())
			.toFile()
			.getAbsolutePath();
		testOutputBasePath = createTempDirectory(SparkDedupTest.class.getSimpleName() + "-")
			.toAbsolutePath()
			.toString();

		FileUtils.deleteDirectory(new File(testOutputBasePath));

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
						SparkStatsTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/profiles/mock_orchestrator.xml")));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("organization")))
			.thenReturn(
				IOUtils
					.toString(
						SparkStatsTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/conf/org.curr.conf.json")));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("publication")))
			.thenReturn(
				IOUtils
					.toString(
						SparkStatsTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/conf/pub.curr.conf.json")));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("software")))
			.thenReturn(
				IOUtils
					.toString(
						SparkStatsTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/conf/sw.curr.conf.json")));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("dataset")))
			.thenReturn(
				IOUtils
					.toString(
						SparkStatsTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/conf/ds.curr.conf.json")));

		lenient()
			.when(isLookUpService.getResourceProfileByQuery(Mockito.contains("otherresearchproduct")))
			.thenReturn(
				IOUtils
					.toString(
						SparkStatsTest.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/dedup/conf/orp.curr.conf.json")));
	}

	@Test
	void createBlockStatsTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkStatsTest.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/createBlockStats_parameters.json")));
		parser
			.parseArgument(
				new String[] {
					"-i", testGraphBasePath,
					"-asi", testActionSetId,
					"-la", "lookupurl",
					"-w", testOutputBasePath
				});

		new SparkBlockStats(parser, spark).run(isLookUpService);

		long orgs_blocks = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/organization_blockstats")
			.count();

		long pubs_blocks = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/publication_blockstats")
			.count();

		long sw_blocks = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/software_blockstats")
			.count();

		long ds_blocks = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/dataset_blockstats")
			.count();

		long orp_blocks = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/otherresearchproduct_blockstats")
			.count();

		assertEquals(414, orgs_blocks);
		assertEquals(187, pubs_blocks);
		assertEquals(128, sw_blocks);
		assertEquals(192, ds_blocks);
		assertEquals(194, orp_blocks);
	}

	@AfterAll
	public static void tearDown() {
		spark.close();
	}
}
