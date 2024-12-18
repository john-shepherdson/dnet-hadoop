
package eu.dnetlib.dhp.actionmanager.fosnodoi;

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

import eu.dnetlib.dhp.actionmanager.createunresolvedentities.GetFOSSparkJob;
import eu.dnetlib.dhp.actionmanager.createunresolvedentities.PrepareTest;
import eu.dnetlib.dhp.actionmanager.createunresolvedentities.ProduceTest;
import eu.dnetlib.dhp.actionmanager.createunresolvedentities.model.FOSDataModel;

/**
 * @author miriam.baglioni
 * @Date 13/02/23
 */
public class GetFosTest {

	private static final Logger log = LoggerFactory.getLogger(ProduceTest.class);

	private static Path workingDir;
	private static SparkSession spark;
	private static LocalFileSystem fs;
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(PrepareTest.class.getSimpleName());

		fs = FileSystem.getLocal(new Configuration());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(ProduceTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(PrepareTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	@Disabled
	void test3() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/fosnodoi/fosnodoi.csv")
			.getPath();

		final String outputPath = workingDir.toString() + "/fos.json";
		GetFOSSparkJob
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", sourcePath,

					"-outputPath", outputPath,
					"-delimiter", ","

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<FOSDataModel> tmp = sc
			.textFile(outputPath)
			.map(item -> OBJECT_MAPPER.readValue(item, FOSDataModel.class));

		tmp.foreach(t -> Assertions.assertTrue(t.getOaid() != null));
		tmp.foreach(t -> Assertions.assertTrue(t.getLevel1() != null));
		tmp.foreach(t -> Assertions.assertTrue(t.getLevel2() != null));
		tmp.foreach(t -> Assertions.assertTrue(t.getLevel3() != null));

		tmp.foreach(t -> System.out.println(new ObjectMapper().writeValueAsString(t)));

	}

}
