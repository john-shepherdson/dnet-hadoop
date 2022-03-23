
package eu.dnetlib.dhp.oa.graph.dump.funderresult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.graph.dump.funderresults.SparkResultLinkedToProject;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Result;

public class ResultLinkedToProjectTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory
		.getLogger(eu.dnetlib.dhp.oa.graph.dump.funderresult.ResultLinkedToProjectTest.class);

	private static final HashMap<String, String> map = new HashMap<>();

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(
				eu.dnetlib.dhp.oa.graph.dump.funderresult.ResultLinkedToProjectTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(eu.dnetlib.dhp.oa.graph.dump.funderresult.ResultLinkedToProjectTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(eu.dnetlib.dhp.oa.graph.dump.funderresult.ResultLinkedToProjectTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testNoMatch() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/funderresource/nomatch/papers.json")
			.getPath();

		final String graphPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/funderresource/preparedInfo")
			.getPath();

		final String communityMapPath = getClass()
				.getResource("/eu/dnetlib/dhp/oa/graph/dump/funderresource/communityMapPath")
				.getPath();

		SparkResultLinkedToProject.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString() + "/preparedInfo",
			"-sourcePath", sourcePath,
			"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Publication",
			"-graphPath", graphPath,
				"-communityMapPath",communityMapPath

		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<CommunityResult> tmp = sc
			.textFile(workingDir.toString() + "/preparedInfo")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		Assertions.assertEquals(0, tmp.count());

	}

	@Test
	void testMatchOne() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/funderresource/match/papers.json")
			.getPath();

		final String graphPath = getClass()
				.getResource("/eu/dnetlib/dhp/oa/graph/dump/funderresource/preparedInfo")
				.getPath();

		final String communityMapPath = getClass()
				.getResource("/eu/dnetlib/dhp/oa/graph/dump/funderresource/communityMapPath")
				.getPath();

		SparkResultLinkedToProject.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString() + "/preparedInfo",
			"-sourcePath", sourcePath,
			"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Publication",
			"-graphPath", graphPath,
				"-communityMapPath", communityMapPath

		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<CommunityResult> tmp = sc
			.textFile(workingDir.toString() + "/preparedInfo")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		Assertions.assertEquals(1, tmp.count());

	}

}
