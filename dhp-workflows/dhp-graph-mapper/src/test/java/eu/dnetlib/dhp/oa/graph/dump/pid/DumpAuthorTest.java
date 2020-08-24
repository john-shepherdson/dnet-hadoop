
package eu.dnetlib.dhp.oa.graph.dump.pid;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
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

import eu.dnetlib.dhp.schema.dump.pidgraph.Entity;

public class DumpAuthorTest {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(DumpAuthorTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(DumpAuthorTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(DumpAuthorTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(DumpAuthorTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void testDumpAuthorPids() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/pid/result")
			.getPath();

		SparkDumpPidAuthor.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString(),
			"-sourcePath", sourcePath,
			"-allowedAuthorPids", "[\"orcid\", \"mag\"]"
		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Entity> tmp = sc
			.textFile(workingDir.toString() + "/author")
			.map(item -> OBJECT_MAPPER.readValue(item, Entity.class));

		org.apache.spark.sql.Dataset<Entity> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Entity.class));

		verificationDataset.show(false);
		Assertions.assertEquals(3, verificationDataset.count());

		Assertions.assertEquals(1, verificationDataset.filter("id = 'mag:fakemag'").count());

		Assertions.assertEquals(2, verificationDataset.filter("substr(id,1,5) = 'orcid'").count());

	}

}
