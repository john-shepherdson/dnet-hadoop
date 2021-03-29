
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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
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

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import jdk.nashorn.internal.ir.annotations.Ignore;

@ExtendWith(MockitoExtension.class)
public class SparkOpenorgsTest implements Serializable {

	@Mock(serializable = true)
	ISLookUpService isLookUpService;

	private static SparkSession spark;
	private static JavaSparkContext jsc;

	private static String testGraphBasePath;
	private static String testOutputBasePath;
	private static String testDedupGraphBasePath;
	private static final String testActionSetId = "test-orchestrator";

	protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	@BeforeAll
	public static void cleanUp() throws IOException, URISyntaxException {

		testGraphBasePath = Paths
			.get(SparkOpenorgsTest.class.getResource("/eu/dnetlib/dhp/dedup/openorgs").toURI())
			.toFile()
			.getAbsolutePath();
		testOutputBasePath = createTempDirectory(SparkOpenorgsTest.class.getSimpleName() + "-")
			.toAbsolutePath()
			.toString();
		testDedupGraphBasePath = createTempDirectory(SparkOpenorgsTest.class.getSimpleName() + "-")
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

	@Disabled
	@Test
	public void copyOpenorgsTest() throws Exception {

		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCopyOpenorgs.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/copyOpenorgs_parameters.json")));
		parser
			.parseArgument(
				new String[] {
					"-i", testGraphBasePath,
					"-asi", testActionSetId,
					"-w", testOutputBasePath,
					"-np", "50"
				});

		new SparkCopyOpenorgs(parser, spark).run(isLookUpService);

		long orgs_deduprecord = jsc
			.textFile(testOutputBasePath + "/" + testActionSetId + "/organization_deduprecord")
			.count();

		assertEquals(100, orgs_deduprecord);
	}

	@Test
	public void copyOpenorgsMergeRels() throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCopyOpenorgsMergeRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/copyOpenorgsMergeRels_parameters.json")));
		parser
			.parseArgument(
				new String[] {
					"-i", testGraphBasePath,
					"-asi", testActionSetId,
					"-w", testOutputBasePath,
					"-la", "lookupurl",
					"-np", "50"
				});

		new SparkCopyOpenorgsMergeRels(parser, spark).run(isLookUpService);

		long orgs_mergerel = spark
			.read()
			.load(testOutputBasePath + "/" + testActionSetId + "/organization_mergerel")
			.count();

		assertEquals(384, orgs_mergerel);

	}

	@Test
	public void copyOpenorgsSimRels() throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCopyOpenorgsSimRels.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/copyOpenorgsMergeRels_parameters.json")));
		parser
			.parseArgument(
				new String[] {
					"-i", testGraphBasePath,
					"-asi", testActionSetId,
					"-w", testOutputBasePath,
					"-la", "lookupurl",
					"-np", "50"
				});

		new SparkCopyOpenorgsSimRels(parser, spark).run(isLookUpService);

		long orgs_simrel = spark
			.read()
			.textFile(testOutputBasePath + "/" + testActionSetId + "/organization_simrel")
			.count();

		assertEquals(73, orgs_simrel);
	}

	@Test
	public void copyRelationsNoOpenorgsTest() throws Exception {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkCopyRelationsNoOpenorgs.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/dedup/updateEntity_parameters.json")));
		parser
			.parseArgument(
				new String[] {
					"-i", testGraphBasePath,
					"-w", testOutputBasePath,
					"-o", testDedupGraphBasePath
				});

		new SparkCopyRelationsNoOpenorgs(parser, spark).run(isLookUpService);

		long relations = jsc.textFile(testDedupGraphBasePath + "/relation").count();

		assertEquals(400, relations);
	}

	@AfterAll
	public static void finalCleanUp() throws IOException {
		FileUtils.deleteDirectory(new File(testOutputBasePath));
		FileUtils.deleteDirectory(new File(testDedupGraphBasePath));
	}

	private static MapFunction<String, Relation> patchRelFn() {
		return value -> {
			final Relation rel = OBJECT_MAPPER.readValue(value, Relation.class);
			if (rel.getDataInfo() == null) {
				rel.setDataInfo(new DataInfo());
			}
			return rel;
		};
	}
}
