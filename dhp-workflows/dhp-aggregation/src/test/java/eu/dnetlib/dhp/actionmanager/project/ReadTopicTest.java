
package eu.dnetlib.dhp.actionmanager.project;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.utils.ReadProjects;
import eu.dnetlib.dhp.actionmanager.project.utils.ReadTopics;
import eu.dnetlib.dhp.actionmanager.project.utils.model.JsonTopic;
import eu.dnetlib.dhp.actionmanager.project.utils.model.Project;

/**

* @author miriam.baglioni

* @Date 01/03/23 

*/
public class ReadTopicTest {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static Path workingDir;

	private static LocalFileSystem fs;

	private static SparkSession spark;

	private static final Logger log = LoggerFactory
		.getLogger(ReadTopicTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(ReadTopicTest.class.getSimpleName());

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

	@Disabled
	@Test
	void readTopics() throws IOException {
		String topics = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/project/topics.json")
			.getPath();
		ReadTopics.readTopics(topics, workingDir.toString() + "/topics", fs);

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<JsonTopic> tmp = sc
			.textFile(workingDir.toString() + "/topics")
			.map(item -> OBJECT_MAPPER.readValue(item, JsonTopic.class));

		// Assertions.assertEquals(16, tmp.count());

		JsonTopic topic = tmp.filter(t -> t.getProjectID().equals("886988")).first();

		Assertions.assertEquals("Individual Fellowships", topic.getTitle());
		Assertions.assertEquals("MSCA-IF-2019", topic.getTopic());

		// tmp.foreach(p -> System.out.println(OBJECT_MAPPER.writeValueAsString(p)));

	}
}
