
package eu.dnetlib.dhp.actionmanager.project;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Project;

public class SparkUpdateProjectTest {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final ClassLoader cl = eu.dnetlib.dhp.actionmanager.project.SparkUpdateProjectTest.class
		.getClassLoader();

	private static SparkSession spark;

	private static Path workingDir;
	private static final Logger log = LoggerFactory
		.getLogger(eu.dnetlib.dhp.actionmanager.project.SparkUpdateProjectTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(eu.dnetlib.dhp.actionmanager.project.SparkUpdateProjectTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(eu.dnetlib.dhp.actionmanager.project.SparkUpdateProjectTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(SparkUpdateProjectTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void numberDistinctProgrammeTest() throws Exception {
		SparkAtomicActionJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-programmePath",
					getClass()
						.getResource("/eu/dnetlib/dhp/actionmanager/project/preparedProgramme_whole.json.gz")
						.getPath(),
					"-projectPath",
					getClass().getResource("/eu/dnetlib/dhp/actionmanager/project/prepared_projects.json").getPath(),
					"-outputPath",
					workingDir.toString() + "/actionSet"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Project> tmp = sc
				.sequenceFile(workingDir.toString() + "/actionSet", Text.class, Text.class)
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
				.map(aa -> ((Project)aa.getPayload()))
			;

		Assertions.assertEquals(14, tmp.count());

//        Dataset<CSVProgramme> verificationDataset = spark.createDataset(tmp.rdd(), Encoders.bean(CSVProgramme.class));
//
//        Assertions.assertEquals(0, verificationDataset.filter("shortTitle =''").count());
	}
}
