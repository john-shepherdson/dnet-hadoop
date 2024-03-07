
package eu.dnetlib.dhp.actionmanager.fosnodoi;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
import eu.dnetlib.dhp.schema.oaf.OafEntity;

public class CreateActionSetSparkJobTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(CreateActionSetSparkJobTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(CreateActionSetSparkJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(CreateActionSetSparkJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(CreateActionSetSparkJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	private void runJob(String resultsInputPath, String outputPath) throws Exception {
		CreateActionSetSparkJob
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", resultsInputPath,

					"-outputPath", outputPath,
				});
	}

	@Test
	void testScores() throws Exception {

		String resultsInputPath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/fosnodoi/results.json")
			.getPath();

		String outputPath = workingDir.toString() + "/actionSet";

		// execute the job to generate the action sets for result scores
		runJob(resultsInputPath, outputPath);
		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		sc
			.sequenceFile(outputPath, Text.class, Text.class)
			.foreach(t -> System.out.println(new ObjectMapper().writeValueAsString(t)));

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
