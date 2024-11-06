
package eu.dnetlib.dhp.resulttocommunityfromorganization;

import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
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

import eu.dnetlib.dhp.schema.oaf.Dataset;

public class ResultToCommunityJobTest {

	private static final Logger log = LoggerFactory.getLogger(ResultToCommunityJobTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(ResultToCommunityJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(ResultToCommunityJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(ResultToCommunityJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testSparkResultToCommunityFromOrganizationJob() throws Exception {
		final String preparedInfoPath = getClass()
			.getResource("/eu/dnetlib/dhp/resulttocommunityfromorganization/preparedInfo")
			.getPath();
		SparkResultToCommunityFromOrganizationJob
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", getClass()
						.getResource("/eu/dnetlib/dhp/resulttocommunityfromorganization/sample/")
						.getPath(),

					"-outputPath", workingDir.toString() + "/",
					"-preparedInfoPath", preparedInfoPath
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(10, tmp.count());
		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));

		verificationDataset.createOrReplaceTempView("dataset");

		String query = "select id, MyT.id community "
			+ "from dataset "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD "
			+ "where MyD.inferenceprovenance = 'propagation'";

		org.apache.spark.sql.Dataset<Row> resultExplodedProvenance = spark.sql(query);
		Assertions.assertEquals(5, resultExplodedProvenance.count());
		Assertions
			.assertEquals(
				0,
				resultExplodedProvenance
					.filter("id = '50|dedup_wf_001::afaf128022d29872c4dad402b2db04fe'")
					.count());
		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter("id = '50|dedup_wf_001::3f62cfc27024d564ea86760c494ba93b'")
					.count());
		Assertions
			.assertEquals(
				"beopen",
				resultExplodedProvenance
					.select("community")
					.where(
						resultExplodedProvenance
							.col("id")
							.equalTo(
								"50|dedup_wf_001::3f62cfc27024d564ea86760c494ba93b"))
					.collectAsList()
					.get(0)
					.getString(0));

		Assertions
			.assertEquals(
				2,
				resultExplodedProvenance
					.filter("id = '50|od________18::8887b1df8b563c4ea851eb9c882c9d7b'")
					.count());
		Assertions
			.assertEquals(
				"mes",
				resultExplodedProvenance
					.select("community")
					.where(
						resultExplodedProvenance
							.col("id")
							.equalTo(
								"50|od________18::8887b1df8b563c4ea851eb9c882c9d7b"))
					.sort(desc("community"))
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"euromarine",
				resultExplodedProvenance
					.select("community")
					.where(
						resultExplodedProvenance
							.col("id")
							.equalTo(
								"50|od________18::8887b1df8b563c4ea851eb9c882c9d7b"))
					.sort(desc("community"))
					.collectAsList()
					.get(1)
					.getString(0));

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter("id = '50|doajarticles::8d817039a63710fcf97e30f14662c6c8'")
					.count());
		Assertions
			.assertEquals(
				"mes",
				resultExplodedProvenance
					.select("community")
					.where(
						resultExplodedProvenance
							.col("id")
							.equalTo(
								"50|doajarticles::8d817039a63710fcf97e30f14662c6c8"))
					.sort(desc("community"))
					.collectAsList()
					.get(0)
					.getString(0));

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter("id = '50|doajarticles::3c98f0632f1875b4979e552ba3aa01e6'")
					.count());
		Assertions
			.assertEquals(
				"mes",
				resultExplodedProvenance
					.select("community")
					.where(
						resultExplodedProvenance
							.col("id")
							.equalTo(
								"50|doajarticles::3c98f0632f1875b4979e552ba3aa01e6"))
					.sort(desc("community"))
					.collectAsList()
					.get(0)
					.getString(0));

		query = "select id, MyT.id community "
			+ "from dataset "
			+ "lateral view explode(context) c as MyT "
			+ "lateral view explode(MyT.datainfo) d as MyD ";

		org.apache.spark.sql.Dataset<Row> resultCommunityId = spark.sql(query);

		Assertions.assertEquals(10, resultCommunityId.count());

		Assertions
			.assertEquals(
				1,
				resultCommunityId
					.filter("id = '50|dedup_wf_001::afaf128022d29872c4dad402b2db04fe'")
					.count());
		Assertions
			.assertEquals(
				"beopen",
				resultCommunityId
					.select("community")
					.where(
						resultCommunityId
							.col("id")
							.equalTo(
								"50|dedup_wf_001::afaf128022d29872c4dad402b2db04fe"))
					.collectAsList()
					.get(0)
					.getString(0));

		Assertions
			.assertEquals(
				1,
				resultCommunityId
					.filter("id = '50|dedup_wf_001::3f62cfc27024d564ea86760c494ba93b'")
					.count());

		Assertions
			.assertEquals(
				3,
				resultCommunityId
					.filter("id = '50|od________18::8887b1df8b563c4ea851eb9c882c9d7b'")
					.count());
		Assertions
			.assertEquals(
				"beopen",
				resultCommunityId
					.select("community")
					.where(
						resultCommunityId
							.col("id")
							.equalTo(
								"50|od________18::8887b1df8b563c4ea851eb9c882c9d7b"))
					.sort(desc("community"))
					.collectAsList()
					.get(2)
					.getString(0));

		Assertions
			.assertEquals(
				2,
				resultCommunityId
					.filter("id = '50|doajarticles::8d817039a63710fcf97e30f14662c6c8'")
					.count());
		Assertions
			.assertEquals(
				"euromarine",
				resultCommunityId
					.select("community")
					.where(
						resultCommunityId
							.col("id")
							.equalTo(
								"50|doajarticles::8d817039a63710fcf97e30f14662c6c8"))
					.sort(desc("community"))
					.collectAsList()
					.get(1)
					.getString(0));

		Assertions
			.assertEquals(
				3,
				resultCommunityId
					.filter("id = '50|doajarticles::3c98f0632f1875b4979e552ba3aa01e6'")
					.count());
		Assertions
			.assertEquals(
				"euromarine",
				resultCommunityId
					.select("community")
					.where(
						resultCommunityId
							.col("id")
							.equalTo(
								"50|doajarticles::3c98f0632f1875b4979e552ba3aa01e6"))
					.sort(desc("community"))
					.collectAsList()
					.get(2)
					.getString(0));
		Assertions
			.assertEquals(
				"ni",
				resultCommunityId
					.select("community")
					.where(
						resultCommunityId
							.col("id")
							.equalTo(
								"50|doajarticles::3c98f0632f1875b4979e552ba3aa01e6"))
					.sort(desc("community"))
					.collectAsList()
					.get(0)
					.getString(0));
	}
}
