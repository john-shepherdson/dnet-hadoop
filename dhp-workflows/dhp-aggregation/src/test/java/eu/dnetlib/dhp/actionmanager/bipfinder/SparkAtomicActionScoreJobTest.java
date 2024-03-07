
package eu.dnetlib.dhp.actionmanager.bipfinder;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.xml.crypto.Data;

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
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Result;

public class SparkAtomicActionScoreJobTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(SparkAtomicActionScoreJobTest.class);

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

	private void runJob(String resultsInputPath, String projectsInputPath, String outputPath) throws Exception {
		SparkAtomicActionScoreJob
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-resultsInputPath", resultsInputPath,
					"-projectsInputPath", projectsInputPath,
					"-outputPath", outputPath,
				});
	}

	@Test
	void testScores() throws Exception {

		String resultsInputPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/bipfinder/result_bip_scores.json")
			.getPath();

		String projectsInputPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/bipfinder/project_bip_scores.json")
			.getPath();

		String outputPath = workingDir.toString() + "/actionSet";

		// execute the job to generate the action sets for result scores
		runJob(resultsInputPath, projectsInputPath, outputPath);

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<OafEntity> tmp = sc
			.sequenceFile(outputPath, Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((OafEntity) aa.getPayload()));

		assertEquals(8, tmp.count());

		Dataset<OafEntity> verificationDataset = spark.createDataset(tmp.rdd(), Encoders.bean(OafEntity.class));
		verificationDataset.createOrReplaceTempView("result");

		Dataset<Row> testDataset = spark
			.sql(
				"Select p.id oaid, mes.id, mUnit.value from result p " +
					"lateral view explode(measures) m as mes " +
					"lateral view explode(mes.unit) u as mUnit ");

//		execVerification.show();

		Assertions.assertEquals(28, testDataset.count());

		assertResultImpactScores(testDataset);
		assertProjectImpactScores(testDataset);

	}

	void assertResultImpactScores(Dataset<Row> testDataset) {
		Assertions
			.assertEquals(
				"6.63451994567e-09", testDataset
					.filter(
						"oaid='50|arXiv_dedup_::4a2d5fd8d71daec016c176ec71d957b1' " +
							"and id = 'influence'")
					.select("value")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"0.348694533145", testDataset
					.filter(
						"oaid='50|arXiv_dedup_::4a2d5fd8d71daec016c176ec71d957b1' " +
							"and id = 'popularity_alt'")
					.select("value")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"2.16094680115e-09", testDataset
					.filter(
						"oaid='50|arXiv_dedup_::4a2d5fd8d71daec016c176ec71d957b1' " +
							"and id = 'popularity'")
					.select("value")
					.collectAsList()
					.get(0)
					.getString(0));
	}

	void assertProjectImpactScores(Dataset<Row> testDataset) throws Exception {

		Assertions
			.assertEquals(
				"0", testDataset
					.filter(
						"oaid='40|nih_________::c02a8233e9b60f05bb418f0c9b714833' " +
							"and id = 'numOfInfluentialResults'")
					.select("value")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"1", testDataset
					.filter(
						"oaid='40|nih_________::c02a8233e9b60f05bb418f0c9b714833' " +
							"and id = 'numOfPopularResults'")
					.select("value")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"25", testDataset
					.filter(
						"oaid='40|nih_________::c02a8233e9b60f05bb418f0c9b714833' " +
							"and id = 'totalImpulse'")
					.select("value")
					.collectAsList()
					.get(0)
					.getString(0));
		Assertions
			.assertEquals(
				"43", testDataset
					.filter(
						"oaid='40|nih_________::c02a8233e9b60f05bb418f0c9b714833' " +
							"and id = 'totalCitationCount'")
					.select("value")
					.collectAsList()
					.get(0)
					.getString(0));
	}
}
