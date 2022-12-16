
package eu.dnetlib.dhp.oa.graph.clean.cfhb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Publication;

public class CleanCfHbSparkJobTest {

	private static final Logger log = LoggerFactory.getLogger(CleanCfHbSparkJobTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path testBaseTmpPath;

	private static String resolvedPath;

	private static String graphInputPath;

	private static String graphOutputPath;

	private static String dsMasterDuplicatePath;

	@BeforeAll
	public static void beforeAll() throws IOException, URISyntaxException {

		testBaseTmpPath = Files.createTempDirectory(CleanCfHbSparkJobTest.class.getSimpleName());
		log.info("using test base path {}", testBaseTmpPath);

		final File entitiesSources = Paths
			.get(CleanCfHbSparkJobTest.class.getResource("/eu/dnetlib/dhp/oa/graph/clean/cfhb/entities").toURI())
			.toFile();

		FileUtils
			.copyDirectory(
				entitiesSources,
				testBaseTmpPath.resolve("input").resolve("entities").toFile());

		FileUtils
			.copyFileToDirectory(
				Paths
					.get(
						CleanCfHbSparkJobTest.class
							.getResource("/eu/dnetlib/dhp/oa/graph/clean/cfhb/masterduplicate.json")
							.toURI())
					.toFile(),
				testBaseTmpPath.resolve("workingDir").resolve("masterduplicate").toFile());

		graphInputPath = testBaseTmpPath.resolve("input").resolve("entities").toString();
		resolvedPath = testBaseTmpPath.resolve("workingDir").resolve("cfHbResolved").toString();
		graphOutputPath = testBaseTmpPath.resolve("workingDir").resolve("cfHbPatched").toString();
		dsMasterDuplicatePath = testBaseTmpPath.resolve("workingDir").resolve("masterduplicate").toString();

		SparkConf conf = new SparkConf();
		conf.setAppName(CleanCfHbSparkJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("spark.ui.enabled", "false");

		spark = SparkSession
			.builder()
			.appName(CleanCfHbSparkJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(testBaseTmpPath.toFile());
		spark.stop();
	}

	@Test
	void testCleanCfHbSparkJob() throws Exception {
		final String outputPath = graphOutputPath + "/dataset";
		final String inputPath = graphInputPath + "/dataset";

		org.apache.spark.sql.Dataset<Dataset> records = read(spark, inputPath, Dataset.class);
		Dataset d = records
			.filter("id = '50|doi_________::09821844208a5cd6300b2bfb13bca1b9'")
			.first();
		assertEquals("10|re3data_____::4c4416659cb74c2e0e891a883a047cbc", d.getCollectedfrom().get(0).getKey());
		assertEquals("Bacterial Protein Interaction Database - DUP", d.getCollectedfrom().get(0).getValue());
		assertEquals(
			"10|re3data_____::4c4416659cb74c2e0e891a883a047cbc", d.getInstance().get(0).getCollectedfrom().getKey());
		assertEquals(
			"Bacterial Protein Interaction Database - DUP", d.getInstance().get(0).getCollectedfrom().getValue());

		d = records
			.filter("id = '50|DansKnawCris::0dd644304b7116e8e58da3a5e3adc37a'")
			.first();
		assertEquals("10|opendoar____::788b4ac1e172d8e520c2b9461c0a3d35", d.getCollectedfrom().get(0).getKey());
		assertEquals("FILUR DATA - DUP", d.getCollectedfrom().get(0).getValue());
		assertEquals(
			"10|opendoar____::788b4ac1e172d8e520c2b9461c0a3d35", d.getInstance().get(0).getCollectedfrom().getKey());
		assertEquals("FILUR DATA - DUP", d.getInstance().get(0).getCollectedfrom().getValue());
		assertEquals(
			"10|re3data_____::6ffd7bc058f762912dc494cd9c175341", d.getInstance().get(0).getHostedby().getKey());
		assertEquals("depositar - DUP", d.getInstance().get(0).getHostedby().getValue());

		d = records
			.filter("id = '50|DansKnawCris::203a27996ddc0fd1948258e5b7dec61c'")
			.first();
		assertEquals("10|openaire____::c6df70599aa984f16ee52b4b86d2e89f", d.getCollectedfrom().get(0).getKey());
		assertEquals("DANS (Data Archiving and Networked Services)", d.getCollectedfrom().get(0).getValue());
		assertEquals(
			"10|openaire____::c6df70599aa984f16ee52b4b86d2e89f", d.getInstance().get(0).getCollectedfrom().getKey());
		assertEquals(
			"DANS (Data Archiving and Networked Services)", d.getInstance().get(0).getCollectedfrom().getValue());
		assertEquals(
			"10|openaire____::c6df70599aa984f16ee52b4b86d2e89f", d.getInstance().get(0).getHostedby().getKey());
		assertEquals("DANS (Data Archiving and Networked Services)", d.getInstance().get(0).getHostedby().getValue());

		CleanCfHbSparkJob
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--inputPath", inputPath,
					"--outputPath", outputPath,
					"--resolvedPath", resolvedPath + "/dataset",
					"--graphTableClassName", Dataset.class.getCanonicalName(),
					"--masterDuplicatePath", dsMasterDuplicatePath
				});

		assertTrue(Files.exists(Paths.get(graphOutputPath, "dataset")));

		records = read(spark, outputPath, Dataset.class);

		assertEquals(3, records.count());

		d = records
			.filter("id = '50|doi_________::09821844208a5cd6300b2bfb13bca1b9'")
			.first();
		assertEquals("10|fairsharing_::a29d1598024f9e87beab4b98411d48ce", d.getCollectedfrom().get(0).getKey());
		assertEquals("Bacterial Protein Interaction Database", d.getCollectedfrom().get(0).getValue());
		assertEquals(
			"10|fairsharing_::a29d1598024f9e87beab4b98411d48ce", d.getInstance().get(0).getCollectedfrom().getKey());
		assertEquals("Bacterial Protein Interaction Database", d.getInstance().get(0).getCollectedfrom().getValue());

		d = records
			.filter("id = '50|DansKnawCris::0dd644304b7116e8e58da3a5e3adc37a'")
			.first();
		assertEquals("10|re3data_____::fc1db64b3964826913b1e9eafe830490", d.getCollectedfrom().get(0).getKey());
		assertEquals("FULIR Data", d.getCollectedfrom().get(0).getValue());
		assertEquals(
			"10|re3data_____::fc1db64b3964826913b1e9eafe830490", d.getInstance().get(0).getCollectedfrom().getKey());
		assertEquals("FULIR Data", d.getInstance().get(0).getCollectedfrom().getValue());
		assertEquals(
			"10|fairsharing_::3f647cadf56541fb9513cb63ec370187", d.getInstance().get(0).getHostedby().getKey());
		assertEquals("depositar", d.getInstance().get(0).getHostedby().getValue());

		d = records
			.filter("id = '50|DansKnawCris::203a27996ddc0fd1948258e5b7dec61c'")
			.first();
		assertEquals("10|openaire____::c6df70599aa984f16ee52b4b86d2e89f", d.getCollectedfrom().get(0).getKey());
		assertEquals("DANS (Data Archiving and Networked Services)", d.getCollectedfrom().get(0).getValue());
		assertEquals(
			"10|openaire____::c6df70599aa984f16ee52b4b86d2e89f", d.getInstance().get(0).getCollectedfrom().getKey());
		assertEquals(
			"DANS (Data Archiving and Networked Services)", d.getInstance().get(0).getCollectedfrom().getValue());
		assertEquals(
			"10|openaire____::c6df70599aa984f16ee52b4b86d2e89f", d.getInstance().get(0).getHostedby().getKey());
		assertEquals("DANS (Data Archiving and Networked Services)", d.getInstance().get(0).getHostedby().getValue());

		d = records
			.filter("id = '50|DansKnawCris::203a27996ddc0fd1948258e5b7dec61c'")
			.first();
		assertEquals("10|openaire____::c6df70599aa984f16ee52b4b86d2e89f", d.getCollectedfrom().get(0).getKey());
		assertEquals("DANS (Data Archiving and Networked Services)", d.getCollectedfrom().get(0).getValue());
		assertEquals(
			"10|openaire____::c6df70599aa984f16ee52b4b86d2e89f", d.getInstance().get(0).getCollectedfrom().getKey());
		assertEquals(
			"DANS (Data Archiving and Networked Services)", d.getInstance().get(0).getCollectedfrom().getValue());
		assertEquals(
			"10|openaire____::c6df70599aa984f16ee52b4b86d2e89f", d.getInstance().get(0).getHostedby().getKey());
		assertEquals("DANS (Data Archiving and Networked Services)", d.getInstance().get(0).getHostedby().getValue());
	}

	private <R> org.apache.spark.sql.Dataset<R> read(SparkSession spark, String path, Class<R> clazz) {
		return spark
			.read()
			.textFile(path)
			.map(as(clazz), Encoders.bean(clazz));
	}

	private static <R> MapFunction<String, R> as(Class<R> clazz) {
		return s -> OBJECT_MAPPER.readValue(s, clazz);
	}
}
