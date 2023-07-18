
package eu.dnetlib.dhp.actionmanager.project;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.utils.model.CSVProject;
import eu.dnetlib.dhp.actionmanager.project.utils.model.Project;

public class PrepareProjectTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final ClassLoader cl = PrepareProjectTest.class
		.getClassLoader();

	private static SparkSession spark;

	private static Path workingDir;
	private static final Logger log = LoggerFactory
		.getLogger(PrepareProjectTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(PrepareProjectTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(PrepareProjectTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(PrepareProjectTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void numberDistinctProjectTest() throws Exception {
		PrepareProjects
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-projectPath",
					getClass().getResource("/eu/dnetlib/dhp/actionmanager/project/projects_nld.json.gz").getPath(),
					"-outputPath",
					workingDir.toString() + "/preparedProjects",
					"-dbProjectPath",
					getClass().getResource("/eu/dnetlib/dhp/actionmanager/project/dbProject").getPath(),

				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<CSVProject> tmp = sc
			.textFile(workingDir.toString() + "/preparedProjects")
			.map(item -> OBJECT_MAPPER.readValue(item, CSVProject.class));

		Assertions.assertEquals(8, tmp.count());

		Dataset<CSVProject> verificationDataset = spark.createDataset(tmp.rdd(), Encoders.bean(CSVProject.class));

		Assertions.assertEquals(0, verificationDataset.filter("length(id) = 0").count());
		Assertions.assertEquals(0, verificationDataset.filter("length(programme) = 0").count());
		Assertions.assertEquals(0, verificationDataset.filter("length(topics) = 0").count());

		CSVProject project = tmp.filter(p -> p.getId().equals("886828")).first();

		Assertions.assertEquals("H2020-EU.2.3.", project.getProgramme());
		Assertions.assertEquals("EIC-SMEInst-2018-2020", project.getTopics());
	}

}
