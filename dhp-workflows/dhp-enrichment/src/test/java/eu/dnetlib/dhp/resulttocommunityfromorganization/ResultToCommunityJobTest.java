
package eu.dnetlib.dhp.resulttocommunityfromorganization;

import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import eu.dnetlib.dhp.api.Utils;
import eu.dnetlib.dhp.api.model.CommunityEntityMap;
import eu.dnetlib.dhp.orcidtoresultfromsemrel.OrcidPropagationJobTest;
import eu.dnetlib.dhp.schema.oaf.Dataset;

public class ResultToCommunityJobTest {

	private static final Logger log = LoggerFactory.getLogger(ResultToCommunityJobTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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
			.appName(OrcidPropagationJobTest.class.getSimpleName())
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

	@Test
	void testCommunityOrganizationAPIs() throws IOException {
		final CommunityEntityMap organizationMap = Utils
			.getCommunityOrganization("https://beta.services.openaire.eu/openaire/community/");
		// final CommunityEntityMap organizationMap = Utils.getOrganizationCommunityMap(baseURL);
		List<String> beopenOrgs = Arrays.asList("20|openorgs____::9dd5545aacd3d8019e00c3f837269746",
				"20|openorgs____::11f6b2617abf37fe7193557d77d8cd00",
				"20|openorgs____::9cb5ffc315d7bf0f97b2f0fdc37612aa",
				"20|openorgs____::72162cfc2e7edaf7515c778e04d1952b",
				"20|openorgs____::a86e8b969264b4c92cbf79c289a3f61a",
				"20|openorgs____::600c7afdde615a68e45cfceaf684782d",
				"20|openorgs____::b79247e30e30a8f8532a30e3b816cda9",
				"20|openorgs____::90a0f7c99fb72cd0e014fcdd38c08719",
				"20|openorgs____::581dcea989b861fa0106d4874ecf2d66",
				"20|openorgs____::ad863df6deda1619a25e7fad4a534891",
				"20|openorgs____::9e29fb5b85151a6810ce7256b08475e4",
				"20|openorgs____::ca4e3e4e6767e05b0828ef5f0cdc7292",
				"20|openorgs____::d6b4b35b44951f55747a7139446d21a8",
				"20|openorgs____::8ec069b683b9e9387492ea0c6b88a806");

		beopenOrgs.forEach(org -> Assertions.assertTrue(organizationMap.get(org).contains("beopen")));


	}
}
