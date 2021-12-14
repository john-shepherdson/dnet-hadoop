
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
import eu.dnetlib.dhp.schema.oaf.Result;

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
	void testMatch() throws Exception {
		String bipScoresPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/bipfinder/bip_scores_oid.json")
			.getPath();
		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/bipfinder/publicationoaid.json")
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

		JavaRDD<Result> tmp = sc
			.sequenceFile(workingDir.toString() + "/actionSet", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Result) aa.getPayload()));

		assertEquals(4, tmp.count());

		Dataset<Result> verificationDataset = spark.createDataset(tmp.rdd(), Encoders.bean(Result.class));
		verificationDataset.createOrReplaceTempView("result");

		Dataset<Row> execVerification = spark
			.sql(
				"Select p.id oaid, mes.id, mUnit.value from result p " +
					"lateral view explode(measures) m as mes " +
					"lateral view explode(mes.unit) u as mUnit ");

		Assertions.assertEquals(12, execVerification.count());
		Assertions
			.assertEquals(
				"6.63451994567e-09", execVerification
					.filter(
						"oaid='50|arXiv_dedup_::4a2d5fd8d71daec016c176ec71d957b1' " +
							"and id = 'influence'")
					.select("value")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"0.348694533145", execVerification
					.filter(
						"oaid='50|arXiv_dedup_::4a2d5fd8d71daec016c176ec71d957b1' " +
							"and id = 'popularity_alt'")
					.select("value")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"2.16094680115e-09", execVerification
					.filter(
						"oaid='50|arXiv_dedup_::4a2d5fd8d71daec016c176ec71d957b1' " +
							"and id = 'popularity'")
					.select("value")
					.collectAsList()
					.get(0)
					.getString(0));

	}

}
