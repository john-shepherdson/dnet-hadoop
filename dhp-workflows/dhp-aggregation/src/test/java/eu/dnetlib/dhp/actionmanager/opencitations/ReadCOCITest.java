
package eu.dnetlib.dhp.actionmanager.opencitations;

import static eu.dnetlib.dhp.actionmanager.Constants.DEFAULT_DELIMITER;

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
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.opencitations.model.COCI;
import eu.dnetlib.dhp.schema.oaf.Dataset;

public class ReadCOCITest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;
	private static final Logger log = LoggerFactory
		.getLogger(ReadCOCITest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(ReadCOCITest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(ReadCOCITest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(ReadCOCITest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testReadCOCI() throws Exception {
		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/opencitations/inputFiles")
			.getPath();

		LocalFileSystem fs = FileSystem.getLocal(new Configuration());
		fs
			.copyFromLocalFile(
				false, new org.apache.hadoop.fs.Path(getClass()
					.getResource("/eu/dnetlib/dhp/actionmanager/opencitations/inputFiles/input1.gz")
					.getPath()),
				new org.apache.hadoop.fs.Path(workingDir + "/COCI/input1.gz"));

		fs
			.copyFromLocalFile(
				false, new org.apache.hadoop.fs.Path(getClass()
					.getResource("/eu/dnetlib/dhp/actionmanager/opencitations/inputFiles/input2.gz")
					.getPath()),
				new org.apache.hadoop.fs.Path(workingDir + "/COCI/input2.gz"));

		fs
			.copyFromLocalFile(
				false, new org.apache.hadoop.fs.Path(getClass()
					.getResource("/eu/dnetlib/dhp/actionmanager/opencitations/inputFiles/input3.gz")
					.getPath()),
				new org.apache.hadoop.fs.Path(workingDir + "/COCI/input3.gz"));

		fs
			.copyFromLocalFile(
				false, new org.apache.hadoop.fs.Path(getClass()
					.getResource("/eu/dnetlib/dhp/actionmanager/opencitations/inputFiles/input4.gz")
					.getPath()),
				new org.apache.hadoop.fs.Path(workingDir + "/COCI/input4.gz"));

		fs
			.copyFromLocalFile(
				false, new org.apache.hadoop.fs.Path(getClass()
					.getResource("/eu/dnetlib/dhp/actionmanager/opencitations/inputFiles/input5.gz")
					.getPath()),
				new org.apache.hadoop.fs.Path(workingDir + "/COCI/input5.gz"));

		ReadCOCI
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-workingPath",
					workingDir.toString() + "/COCI",
					"-outputPath",
					workingDir.toString() + "/COCI_json/",
					"-inputFile", "input1;input2;input3;input4;input5"
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<COCI> tmp = sc
			.textFile(workingDir.toString() + "/COCI_json/*/")
			.map(item -> OBJECT_MAPPER.readValue(item, COCI.class));

		Assertions.assertEquals(24, tmp.count());

		Assertions.assertEquals(1, tmp.filter(c -> c.getCiting().equals("10.1207/s15327647jcd3,4-01")).count());

		Assertions.assertEquals(8, tmp.filter(c -> c.getCiting().indexOf(".refs") > -1).count());
	}

}
