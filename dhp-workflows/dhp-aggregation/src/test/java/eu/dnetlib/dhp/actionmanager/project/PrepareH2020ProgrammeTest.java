
package eu.dnetlib.dhp.actionmanager.project;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.utils.model.CSVProgramme;

public class PrepareH2020ProgrammeTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final ClassLoader cl = PrepareH2020ProgrammeTest.class
		.getClassLoader();

	private static SparkSession spark;

	private static Path workingDir;
	private static final Logger log = LoggerFactory
		.getLogger(PrepareH2020ProgrammeTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(PrepareH2020ProgrammeTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(PrepareH2020ProgrammeTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(PrepareH2020ProgrammeTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void numberDistinctProgrammeTest() throws Exception {
		PrepareProgramme
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-programmePath",
					getClass().getResource("/eu/dnetlib/dhp/actionmanager/project/h2020_programme.json.gz").getPath(),
					"-outputPath",
					workingDir.toString() + "/preparedProgramme"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<CSVProgramme> tmp = sc
			.textFile(workingDir.toString() + "/preparedProgramme")
			.map(item -> OBJECT_MAPPER.readValue(item, CSVProgramme.class));

		Assertions.assertEquals(279, tmp.count());

		Dataset<CSVProgramme> verificationDataset = spark.createDataset(tmp.rdd(), Encoders.bean(CSVProgramme.class));

		Assertions.assertEquals(0, verificationDataset.filter("title =''").count());

		Assertions.assertEquals(0, verificationDataset.filter("classification = ''").count());

		// tmp.foreach(csvProgramme -> System.out.println(OBJECT_MAPPER.writeValueAsString(csvProgramme)));

		Assertions
			.assertEquals(
				"Societal challenges | Smart, Green And Integrated Transport | CLEANSKY2 | IADP Fast Rotorcraft",
				verificationDataset
					.filter("code = 'H2020-EU.3.4.5.3.'")
					.select("classification")
					.collectAsList()
					.get(0)
					.getString(0));

		Assertions
			.assertEquals(
				"Euratom | Indirect actions | European Fusion Development Agreement",
				verificationDataset
					.filter("code = 'H2020-Euratom-1.9.'")
					.select("classification")
					.collectAsList()
					.get(0)
					.getString(0));

		Assertions
			.assertEquals(
				"Industrial leadership | Leadership in enabling and industrial technologies | Advanced manufacturing and processing | New sustainable business models",
				verificationDataset
					.filter("code = 'H2020-EU.2.1.5.4.'")
					.select("classification")
					.collectAsList()
					.get(0)
					.getString(0));

		Assertions
			.assertEquals(
				"Excellent science | Future and Emerging Technologies (FET) | FET Open",
				verificationDataset
					.filter("code = 'H2020-EU.1.2.1.'")
					.select("classification")
					.collectAsList()
					.get(0)
					.getString(0));

		Assertions
			.assertEquals(
				"Industrial leadership | Leadership in enabling and industrial technologies | Biotechnology",
				verificationDataset
					.filter("code = 'H2020-EU.2.1.4.'")
					.select("classification")
					.collectAsList()
					.get(0)
					.getString(0));

	}

}
