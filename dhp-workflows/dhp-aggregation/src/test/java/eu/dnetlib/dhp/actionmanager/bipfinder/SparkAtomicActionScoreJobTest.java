
package eu.dnetlib.dhp.actionmanager.bipfinder;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
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

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Publication;

public class SparkAtomicActionScoreJobTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;
	private static final Logger log = LoggerFactory
		.getLogger(SparkAtomicActionScoreJobTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(SparkAtomicActionScoreJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(SparkAtomicActionScoreJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(SparkAtomicActionScoreJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void matchOne() throws Exception {
		String bipScoresPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/bipfinder/bip_scores.json")
			.getPath();
		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/bipfinder/publication.json")
			.getPath();

		SparkAtomicActionScoreJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-inputPath",
					inputPath,
					"-bipScorePath",
					bipScoresPath,
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Publication",
					"-outputPath",
					workingDir.toString() + "/actionSet"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Publication> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Publication) aa.getPayload()));

		assertEquals(1, tmp.count());

		Dataset<Publication> verificationDataset = spark.createDataset(tmp.rdd(), Encoders.bean(Publication.class));
		verificationDataset.createOrReplaceTempView("publication");

		Dataset<Row> execVerification = spark
			.sql(
				"Select p.id oaid, mes.id, mUnit.value from publication p " +
					"lateral view explode(measures) m as mes " +
					"lateral view explode(mes.unit) u as mUnit ");

		Assertions.assertEquals(2, execVerification.count());

		Assertions
			.assertEquals(
				"50|355e65625b88::ffa5bad14f4adc0c9a15c00efbbccddb",
				execVerification.select("oaid").collectAsList().get(0).getString(0));

		Assertions
			.assertEquals(
				"1.47565045883e-08",
				execVerification.filter("id = 'influence'").select("value").collectAsList().get(0).getString(0));

		Assertions
			.assertEquals(
				"0.227515392",
				execVerification.filter("id = 'popularity'").select("value").collectAsList().get(0).getString(0));

	}

	@Test
	void matchOneWithTwo() throws Exception {
		String bipScoresPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/bipfinder/bip_scores.json")
			.getPath();
		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/bipfinder/publication_2.json")
			.getPath();

		SparkAtomicActionScoreJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-inputPath",
					inputPath,
					"-bipScorePath",
					bipScoresPath,
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Publication",
					"-outputPath",
					workingDir.toString() + "/actionSet"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Publication> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Publication) aa.getPayload()));

		assertEquals(1, tmp.count());

		Dataset<Publication> verificationDataset = spark.createDataset(tmp.rdd(), Encoders.bean(Publication.class));
		verificationDataset.createOrReplaceTempView("publication");

		Dataset<Row> execVerification = spark
			.sql(
				"Select p.id oaid, mes.id, mUnit.value from publication p " +
					"lateral view explode(measures) m as mes " +
					"lateral view explode(mes.unit) u as mUnit ");

		Assertions.assertEquals(4, execVerification.count());

		Assertions
			.assertEquals(
				"50|355e65625b88::ffa5bad14f4adc0c9a15c00efbbccddb",
				execVerification.select("oaid").collectAsList().get(0).getString(0));

		Assertions
			.assertEquals(
				2,
				execVerification.filter("id = 'influence'").count());

		Assertions
			.assertEquals(
				2,
				execVerification.filter("id = 'popularity'").count());

		List<Row> tmp_ds = execVerification.filter("id = 'influence'").select("value").collectAsList();
		String tmp_influence = tmp_ds.get(0).getString(0);
		assertTrue(
			"1.47565045883e-08".equals(tmp_influence) ||
				"1.98956540239e-08".equals(tmp_influence));

		tmp_influence = tmp_ds.get(1).getString(0);
		assertTrue(
			"1.47565045883e-08".equals(tmp_influence) ||
				"1.98956540239e-08".equals(tmp_influence));

		assertNotEquals(tmp_ds.get(1).getString(0), tmp_ds.get(0).getString(0));

	}

	@Test
	void matchTwo() throws Exception {
		String bipScoresPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/bipfinder/bip_scores.json")
			.getPath();
		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/bipfinder/publication_3.json")
			.getPath();

		SparkAtomicActionScoreJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-inputPath",
					inputPath,
					"-bipScorePath",
					bipScoresPath,
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Publication",
					"-outputPath",
					workingDir.toString() + "/actionSet"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Publication> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Publication) aa.getPayload()));

		assertEquals(2, tmp.count());

		Dataset<Publication> verificationDataset = spark.createDataset(tmp.rdd(), Encoders.bean(Publication.class));
		verificationDataset.createOrReplaceTempView("publication");

		Dataset<Row> execVerification = spark
			.sql(
				"Select p.id oaid, mes.id, mUnit.value from publication p " +
					"lateral view explode(measures) m as mes " +
					"lateral view explode(mes.unit) u as mUnit ");

		Assertions.assertEquals(4, execVerification.count());

		Assertions
			.assertEquals(
				2,
				execVerification.filter("oaid = '50|355e65625b88::ffa5bad14f4adc0c9a15c00efbbccddb'").count());

		Assertions
			.assertEquals(
				2,
				execVerification.filter("oaid = '50|acm_________::faed5b7a1bd8f51118d13ed29cfaee09'").count());

		Assertions
			.assertEquals(
				2,
				execVerification.filter("id = 'influence'").count());

		Assertions
			.assertEquals(
				2,
				execVerification.filter("id = 'popularity'").count());

		Assertions
			.assertEquals(
				"1.47565045883e-08",
				execVerification
					.filter(
						"oaid = '50|355e65625b88::ffa5bad14f4adc0c9a15c00efbbccddb' " +
							"and id = 'influence'")
					.select("value")
					.collectAsList()
					.get(0)
					.getString(0));

		Assertions
			.assertEquals(
				"1.98956540239e-08",
				execVerification
					.filter(
						"oaid = '50|acm_________::faed5b7a1bd8f51118d13ed29cfaee09' " +
							"and id = 'influence'")
					.select("value")
					.collectAsList()
					.get(0)
					.getString(0));

		Assertions
			.assertEquals(
				"0.282046161584",
				execVerification
					.filter(
						"oaid = '50|acm_________::faed5b7a1bd8f51118d13ed29cfaee09' " +
							"and id = 'popularity'")
					.select("value")
					.collectAsList()
					.get(0)
					.getString(0));

		Assertions
			.assertEquals(
				"0.227515392",
				execVerification
					.filter(
						"oaid = '50|355e65625b88::ffa5bad14f4adc0c9a15c00efbbccddb' " +
							"and id = 'popularity'")
					.select("value")
					.collectAsList()
					.get(0)
					.getString(0));

	}

}
