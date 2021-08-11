
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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

import eu.dnetlib.dhp.oa.graph.dump.community.CommunitySplit;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;

public class SplitForCommunityTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(DumpJobTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(SplitForCommunityTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(SplitForCommunityTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(SplitForCommunityTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void test1() {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/splitForCommunity")
			.getPath();

		final String communityMapPath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/communityMapPath/communitymap.json")
			.getPath();

		CommunitySplit split = new CommunitySplit();

		split.run(false, sourcePath, workingDir.toString() + "/split", communityMapPath);

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<CommunityResult> tmp = sc
			.textFile(workingDir.toString() + "/split/dh-ch")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		org.apache.spark.sql.Dataset<CommunityResult> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(CommunityResult.class));

		Assertions.assertEquals(19, verificationDataset.count());

		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|dedup_wf_001::51b88f272ba9c3bb181af64e70255a80'").count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/egi")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(CommunityResult.class));

		Assertions.assertEquals(1, verificationDataset.count());

		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|dedup_wf_001::e4805d005bfab0cd39a1642cbf477fdb'").count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/ni")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(CommunityResult.class));

		Assertions.assertEquals(5, verificationDataset.count());

		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|datacite____::6b1e3a2fa60ed8c27317a66d6357f795'").count());

		tmp = sc
			.textFile(workingDir.toString() + "/split/science-innovation-policy")
			.map(item -> OBJECT_MAPPER.readValue(item, CommunityResult.class));

		verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(CommunityResult.class));

		Assertions.assertEquals(4, verificationDataset.count());

		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|dedup_wf_001::0347b1cd516fc59e41ba92e0d74e4e9f'").count());
		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|dedup_wf_001::1432beb6171baa5da8a85a7f99545d69'").count());
		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|dedup_wf_001::1c8bd19e633976e314b88ce5c3f92d69'").count());
		Assertions
			.assertEquals(
				1, verificationDataset.filter("id = '50|dedup_wf_001::51b88f272ba9c3bb181af64e70255a80'").count());

	}
}
