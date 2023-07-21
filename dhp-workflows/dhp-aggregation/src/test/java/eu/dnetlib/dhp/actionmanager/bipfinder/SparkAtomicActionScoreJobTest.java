
package eu.dnetlib.dhp.actionmanager.bipfinder;

import static org.junit.jupiter.api.Assertions.*;

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
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Result;

public class SparkAtomicActionScoreJobTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private final static String RESULT = "result";
	private final static String PROJECT = "project";

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

	private void runJob(String inputPath, String outputPath, String targetEntity) throws Exception {
		SparkAtomicActionScoreJob
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-inputPath", inputPath,
					"-outputPath", outputPath,
					"-targetEntity", targetEntity,
				});
	}

	@Test
	void testResultScores() throws Exception {
		final String targetEntity = RESULT;
		String inputResultScores = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/bipfinder/result_bip_scores.json")
			.getPath();
		String outputPath = workingDir.toString() + "/" + targetEntity + "/actionSet";

		// execute the job to generate the action sets for result scores
		runJob(inputResultScores, outputPath, targetEntity);

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Result> tmp = sc
			.sequenceFile(outputPath, Text.class, Text.class)
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

	@Test
	void testProjectScores() throws Exception {
		String targetEntity = PROJECT;
		String inputResultScores = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/bipfinder/project_bip_scores.json")
			.getPath();
		String outputPath = workingDir.toString() + "/" + targetEntity + "/actionSet";

		// execute the job to generate the action sets for project scores
		runJob(inputResultScores, outputPath, PROJECT);

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Project> projects = sc
			.sequenceFile(outputPath, Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Project) aa.getPayload()));

		// test the number of projects
		assertEquals(4, projects.count());

		String testProjectId = "40|nih_________::c02a8233e9b60f05bb418f0c9b714833";

		// count that the project with id testProjectId is present
		assertEquals(1, projects.filter(row -> row.getId().equals(testProjectId)).count());

		projects
			.filter(row -> row.getId().equals(testProjectId))
			.flatMap(r -> r.getMeasures().iterator())
			.foreach(m -> {
				log.info(m.getId() + " " + m.getUnit());

				// ensure that only one score is present for each bip impact measure
				assertEquals(1, m.getUnit().size());

				KeyValue kv = m.getUnit().get(0);

				// ensure that the correct key is provided, i.e. score
				assertEquals("score", kv.getKey());

				switch (m.getId()) {
					case "numOfInfluentialResults":
						assertEquals("0", kv.getValue());
						break;
					case "numOfPopularResults":
						assertEquals("1", kv.getValue());
						break;
					case "totalImpulse":
						assertEquals("25", kv.getValue());
						break;
					case "totalCitationCount":
						assertEquals("43", kv.getValue());
						break;
					default:
						fail("Unknown measure id in the context of projects");
				}
			});
	}
}
