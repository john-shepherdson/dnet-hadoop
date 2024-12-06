
package eu.dnetlib.dhp.person;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.countrypropagation.SparkCountryPropagationJob;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

public class PersonPropagationJobTest {

	private static final Logger log = LoggerFactory.getLogger(PersonPropagationJobTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(PersonPropagationJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(PersonPropagationJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(PersonPropagationJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testPersonPropagation() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/personpropagation/graph")
			.getPath();

		SparkExtractPersonRelations
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", sourcePath,
					"--outputPath", workingDir.toString()
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		// TODO write assertions and find relevant information for hte resource files
	}

}
