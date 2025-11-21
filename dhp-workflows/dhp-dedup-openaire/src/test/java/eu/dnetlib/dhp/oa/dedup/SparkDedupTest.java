
package eu.dnetlib.dhp.oa.dedup;

import static java.nio.file.Files.createTempDirectory;
import static org.mockito.Mockito.lenient;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SparkDedupTest implements Serializable {

	static final boolean CHECK_CARDINALITIES = true;

	@Mock(serializable = true)
	ISLookUpService isLookUpService;

	private static SparkSession spark;
	private static JavaSparkContext jsc;

	private static String testGraphBasePath;
	private static String testOutputBasePath;
	private static String testDedupGraphBasePath;
	private static String testConsistencyGraphBasePath;
	private static String relationsOutputPath;

	private static final String testActionSetId = "test-orchestrator";
	private static String whitelistPath;
	private static List<String> whiteList;

	private static String WHITELIST_SEPARATOR = "####";

	@BeforeAll
	public static void cleanUp() throws IOException, URISyntaxException {

		testGraphBasePath = "/Users/michele/Downloads";

		testOutputBasePath = "/Users/michele/Downloads/authors_dedup";

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

		relationsOutputPath = testOutputBasePath + "/relations_for_broker";

		FileUtils.deleteDirectory(new File(testOutputBasePath));
		FileUtils.deleteDirectory(new File(testDedupGraphBasePath));

		final SparkConf conf = new SparkConf();
		conf.set("spark.sql.shuffle.partitions", "200");
		conf.set("spark.sql.warehouse.dir", testOutputBasePath + "/spark-warehouse");
		conf.set("spark.driver.host", "127.0.0.1");
		conf.set("spark.sql.codegen.wholeStage", "false");
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
				.when(this.isLookUpService.getResourceProfileByQuery(Mockito.contains(testActionSetId)))
				.thenReturn(classPathResourceAsString("/eu/dnetlib/dhp/dedup/profiles/mock_orchestrator.xml"));

		lenient()
				.when(this.isLookUpService.getResourceProfileByQuery(Mockito.contains("authors")))
				.thenReturn(classPathResourceAsString("/eu/dnetlib/dhp/dedup/conf/authors_conf.json"));

	}

	@Test
	@Order(1)
	void createSimRelsTest() throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				classPathResourceAsString("/eu/dnetlib/dhp/oa/dedup/createSimRels_parameters.json"));

		parser
				.parseArgument(new String[] {
						"-i", testGraphBasePath,
						"-asi", testActionSetId,
						"-la", "lookupurl",
						"-w", testOutputBasePath,
						"-np", "50"
				});

		new SparkCreateSimRels(parser, spark).run(this.isLookUpService);
		/*
		 * long orgs_simrel = spark .read() .load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "organization"))
		 * .count();
		 *
		 * long pubs_simrel = spark .read() .load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "publication"))
		 * .count();
		 *
		 * long sw_simrel = spark .read() .load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "software")) .count();
		 *
		 * long ds_simrel = spark .read() .load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "dataset")) .count();
		 *
		 * long orp_simrel = spark .read() .load(DedupUtility.createSimRelPath(testOutputBasePath, testActionSetId, "otherresearchproduct"))
		 * .count();
		 *
		 * System.out.println("orgs_simrel = " + orgs_simrel); System.out.println("pubs_simrel = " + pubs_simrel);
		 * System.out.println("sw_simrel = " + sw_simrel); System.out.println("ds_simrel = " + ds_simrel);
		 * System.out.println("orp_simrel = " + orp_simrel);
		 *
		 * if (CHECK_CARDINALITIES) { assertEquals(720, orgs_simrel); assertEquals(567, pubs_simrel); assertEquals(113, sw_simrel);
		 * assertEquals(148, ds_simrel); assertEquals(280, orp_simrel); }
		 */

	}

	public boolean isDeletedByInference(final String s) {
		return s.contains("\"deletedbyinference\":true");
	}

	private static String classPathResourceAsString(final String path) throws IOException {
		return IOUtils
				.toString(SparkDedupTest.class
						.getResourceAsStream(path));
	}

}
