
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.neethi.Assertion;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.dump.oaf.Result;

public class UpdateProjectInfoTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(eu.dnetlib.dhp.oa.graph.dump.UpdateProjectInfoTest.class);

	private static HashMap<String, String> map = new HashMap<>();

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(eu.dnetlib.dhp.oa.graph.dump.UpdateProjectInfoTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(eu.dnetlib.dhp.oa.graph.dump.UpdateProjectInfoTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(eu.dnetlib.dhp.oa.graph.dump.UpdateProjectInfoTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void test1() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/addProjectInfo")
			.getPath();

		SparkUpdateProjectInfo.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-preparedInfoPath", sourcePath + "/preparedInfo",
			"-outputPath", workingDir.toString() + "/result",
			"-sourcePath", sourcePath + "/software.json"
		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Result> tmp = sc
			.textFile(workingDir.toString() + "/result")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		org.apache.spark.sql.Dataset<Result> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Result.class));

		verificationDataset.show(false);

		Assertions.assertEquals(6, verificationDataset.count());
		verificationDataset.createOrReplaceTempView("dataset");

		String query = "select id, MyT.code code, MyT.title title, MyT.funder.name funderName, MyT.funder.shortName funderShortName, "
			+
			"MyT.funder.jurisdiction funderJurisdiction, MyT.funder.fundingStream fundingStream "
			+ "from dataset " +
			"lateral view explode(projects) p as MyT ";

		org.apache.spark.sql.Dataset<Row> resultExplodedProvenance = spark.sql(query);

		Assertions.assertEquals(3, resultExplodedProvenance.count());
		resultExplodedProvenance.show(false);

		Assertions
			.assertEquals(
				2,
				resultExplodedProvenance.filter("id = '50|dedup_wf_001::e4805d005bfab0cd39a1642cbf477fdb'").count());

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter("id = '50|dedup_wf_001::e4805d005bfab0cd39a1642cbf477fdb' and code = '123455'")
					.count());

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter("id = '50|dedup_wf_001::e4805d005bfab0cd39a1642cbf477fdb' and code = '119027'")
					.count());

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter("id = '50|dedup_wf_001::51b88f272ba9c3bb181af64e70255a80' and code = '123455'")
					.count());

		resultExplodedProvenance.show(false);
	}

}
