
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

public class PreparedInfoPidTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(PreparedInfoPidTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(PreparedInfoPidTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(PreparedInfoPidTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(PreparedInfoPidTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void testPublication() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/pid/result/publication")
			.getPath();

		SparkPrepareResultPids.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString() + "/preparedInfo",
			"-sourcePath", sourcePath,
			"-allowedResultPids", "[\"doi\",\"arxiv\",\"pmc\",\"pmid\",\"pdb\"]",
			"-allowedAuthorPids", "[\"orcid\"]",
			"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Publication"
		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<ResultPidsList> tmp = sc
			.textFile(workingDir.toString() + "/preparedInfo/publication")
			.map(item -> OBJECT_MAPPER.readValue(item, ResultPidsList.class));

		org.apache.spark.sql.Dataset<ResultPidsList> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(ResultPidsList.class));

		Assertions.assertEquals(31, verificationDataset.count());

		verificationDataset.createOrReplaceTempView("check");

		Assertions
			.assertEquals(
				31, spark
					.sql(
						"Select resultId " +
							"from check " +
							"lateral view explode (resultAllowedPids) r as resallowed " +
							" where resallowed.key = 'doi'")
					.count());

		Dataset<Row> check = spark
			.sql(
				"Select resultId, pids.value orcid, resallowed.key pid_type, resallowed.value pid " +
					"from check" +
					" lateral view explode(authorAllowedPids) a as authallowed " +
					" lateral view explode(authallowed) p as pids " +
					" lateral view explode (resultAllowedPids) r as resallowed " +
					"where pids.key = 'orcid'");

		Assertions.assertEquals(2, check.count());

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

	@Test
	public void testDataset() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/pid/result/dataset")
			.getPath();

		SparkPrepareResultPids.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString() + "/preparedInfo",
			"-sourcePath", sourcePath,
			"-allowedResultPids", "[\"doi\",\"arxiv\",\"pmc\",\"pmid\",\"pdb\"]",
			"-allowedAuthorPids", "[\"orcid\"]",
			"-resultTableName", "eu.dnetlib.dhp.schema.oaf.Dataset"
		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<ResultPidsList> tmp = sc
			.textFile(workingDir.toString() + "/preparedInfo/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, ResultPidsList.class));

		org.apache.spark.sql.Dataset<ResultPidsList> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(ResultPidsList.class));

		Assertions.assertEquals(3, verificationDataset.count());

		verificationDataset.createOrReplaceTempView("check");

		Dataset<Row> check = spark
			.sql(
				"Select resultId, pids.value orcid, resallowed.key pid_type, resallowed.value pid " +
					"from check" +
					" lateral view explode(authorAllowedPids) a as authallowed " +
					" lateral view explode(authallowed) p as pids " +
					" lateral view explode (resultAllowedPids) r as resallowed ");

		Assertions.assertEquals(4, check.count());

		Assertions
			.assertEquals(4, check.filter("resultId = '50|_____OmicsDI::00899d9cb1163754943a3277365adc02'").count());
		Assertions
			.assertEquals(
				2, check
					.filter(
						"resultId = '50|_____OmicsDI::00899d9cb1163754943a3277365adc02' " +
							"and (orcid = '0000-0001-9317-9350' or orcid = '0000-0002-1114-4216') and " +
							"pid = '10.1016/fake' and pid_type = 'doi' ")
					.count());

		Assertions
			.assertEquals(
				2, check
					.filter(
						"resultId = '50|_____OmicsDI::00899d9cb1163754943a3277365adc02' " +
							"and (orcid = '0000-0001-9317-9350' or orcid = '0000-0002-1114-4216') and " +
							"pid = '10443fake' and pid_type = 'pmid' ")
					.count());

		check = spark
			.sql(
				"Select resultId, authorAllowedPids, resallowed.key pid_type, resallowed.value pid " +
					"from check" +
					" lateral view explode (resultAllowedPids) r as resallowed ");

		Assertions.assertEquals(5, check.count());

		Assertions
			.assertEquals(2, check.filter("resultId = '50|_____OmicsDI::00899d9cb1163754943a3277365adc02'").count());
		Assertions
			.assertEquals(
				1, check
					.filter(
						"resultId = '50|_____OmicsDI::00899d9cb1163754943a3277365adc02' " +
							" and pid = '10.1016/fake' and pid_type = 'doi' ")
					.count());
		Assertions
			.assertEquals(
				1, check
					.filter(
						"resultId = '50|_____OmicsDI::00899d9cb1163754943a3277365adc02' " +
							" and pid = '10443fake' and pid_type = 'pmid' ")
					.count());

		Assertions
			.assertEquals(1, check.filter("resultId = '50|_____OmicsDI::023fd1fcbb64f0f7df0671798a62f379'").count());
		Assertions
			.assertEquals(
				1, check
					.filter(
						"resultId = '50|_____OmicsDI::023fd1fcbb64f0f7df0671798a62f379' " +
							" and pid = 'fakepdb' and pid_type = 'pdb' ")
					.count());

		Assertions
			.assertEquals(2, check.filter("resultId = '50|_____OmicsDI::036d65211a6ac14237c6e2d7cc223386'").count());
		Assertions
			.assertEquals(
				1, check
					.filter(
						"resultId = '50|_____OmicsDI::036d65211a6ac14237c6e2d7cc223386' " +
							" and pid = '10.1016/j.tcs.2012.06.029' and pid_type = 'doi' ")
					.count());
		Assertions
			.assertEquals(
				1, check
					.filter(
						"resultId = '50|_____OmicsDI::036d65211a6ac14237c6e2d7cc223386' " +
							" and pid = 'fake_arxiv' and pid_type = 'arXiv' ")
					.count());

	}
}
