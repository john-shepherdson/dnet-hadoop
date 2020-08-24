
package eu.dnetlib.dhp.oa.graph.dump.pid;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
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

public class PreparedInfoCollectTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(PreparedInfoCollectTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(PreparedInfoCollectTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(PreparedInfoCollectTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(PreparedInfoCollectTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void testCollectAndSave() throws Exception {
//software and otherresearchproduct associated preparedInfo files intended to be empty
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/pid/preparedInfoSplit")
			.getPath();

		SparkCollectPreparedInfo.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString() + "/preparedInfo",
			"-preparedInfoPath", sourcePath
		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<ResultPidsList> tmp = sc
			.textFile(workingDir.toString() + "/preparedInfo")
			.map(item -> OBJECT_MAPPER.readValue(item, ResultPidsList.class));

		org.apache.spark.sql.Dataset<ResultPidsList> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(ResultPidsList.class));

		Assertions.assertEquals(34, verificationDataset.count());

		verificationDataset.createOrReplaceTempView("check");

		Assertions
			.assertEquals(
				33, spark
					.sql(
						"Select resultId " +
							"from check " +
							"lateral view explode (resultAllowedPids) r as resallowed " +
							" where resallowed.key = 'doi'")
					.count());

		Assertions
			.assertEquals(
				4, spark
					.sql(
						"SELECT pids.value " +
							"FROM check " +
							"LATERAL VIEW EXPLODE (authorAllowedPids) a as authallowed " +
							"LATERAL VIEW EXPLODE (authallowed) a as pids " +
							"WHERE pids.key = 'orcid'")
					.count());

		Assertions
			.assertEquals(
				1, spark
					.sql(
						"SELECT pids.value " +
							"FROM check " +
							"LATERAL VIEW EXPLODE (authorAllowedPids) a as authallowed " +
							"LATERAL VIEW EXPLODE (authallowed) a as pids " +
							"WHERE pids.key = 'mag'")
					.count());

		Dataset<Row> check = spark
			.sql(
				"Select resultId, pids.value orcid, resallowed.key pid_type, resallowed.value pid " +
					"from check" +
					" lateral view explode(authorAllowedPids) a as authallowed " +
					" lateral view explode(authallowed) p as pids " +
					" lateral view explode (resultAllowedPids) r as resallowed " +
					"where pids.key = 'orcid'");

		Assertions.assertEquals(6, check.count());

		Assertions
			.assertEquals(1, check.filter("resultId = '50|base_oa_____::1a700385222228181f20835bae60a71e'").count());
		Assertions
			.assertEquals(
				1, check
					.filter(
						"resultId = '50|base_oa_____::1a700385222228181f20835bae60a71e' " +
							"and orcid = '0000-0002-1114-4216'")
					.count());
		Assertions
			.assertEquals(
				1, check
					.filter(
						"resultId = '50|base_oa_____::1a700385222228181f20835bae60a71e' " +
							"and pid = '10.1016/j.jrmge.2016.11.005'")
					.count());

		Assertions
			.assertEquals(1, check.filter("resultId = '50|base_oa_____::06546a1ad6b1c71e5e366ef15b9ade1f'").count());
		Assertions
			.assertEquals(
				1, check
					.filter(
						"resultId = '50|base_oa_____::06546a1ad6b1c71e5e366ef15b9ade1f' " +
							"and orcid = '0000-0001-9317-9350'")
					.count());
		Assertions
			.assertEquals(
				1, check
					.filter(
						"resultId = '50|base_oa_____::06546a1ad6b1c71e5e366ef15b9ade1f' " +
							"and pid = '10.1016/j.jotr.2014.10.003'")
					.count());

	}

}
