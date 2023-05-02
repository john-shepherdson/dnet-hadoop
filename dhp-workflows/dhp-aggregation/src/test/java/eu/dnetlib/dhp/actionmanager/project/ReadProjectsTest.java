
package eu.dnetlib.dhp.actionmanager.project;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.utils.ReadProjects;
import eu.dnetlib.dhp.actionmanager.project.utils.model.CSVProject;
import eu.dnetlib.dhp.actionmanager.project.utils.model.Project;

/**
 * @author miriam.baglioni
 * @Date 01/03/23
 */
public class ReadProjectsTest {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static Path workingDir;

	private static LocalFileSystem fs;

	private static SparkSession spark;

	private static final Logger log = LoggerFactory
		.getLogger(ReadProjectsTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(ReadProjectsTest.class.getSimpleName());

		fs = FileSystem.getLocal(new Configuration());
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
	void readProjects() throws IOException {
		String projects = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/project/projects.json")
			.getPath();
		ReadProjects.readProjects(projects, workingDir.toString() + "/projects", fs);

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Project> tmp = sc
			.textFile(workingDir.toString() + "/projects")
			.map(item -> OBJECT_MAPPER.readValue(item, Project.class));

		Assertions.assertEquals(19, tmp.count());

		Project project = tmp.filter(p -> p.getAcronym().equals("GiSTDS")).first();

		Assertions.assertEquals("2022-10-08 18:28:27", project.getContentUpdateDate());
		Assertions.assertEquals("894593", project.getId());
		Assertions.assertEquals("H2020-EU.1.3.", project.getLegalBasis());
		Assertions.assertEquals("MSCA-IF-2019", project.getTopics());

		// tmp.foreach(p -> System.out.println(OBJECT_MAPPER.writeValueAsString(p)));

	}
}
