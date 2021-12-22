
package eu.dnetlib.dhp.actionmanager.createunresolvedentities;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.createunresolvedentities.model.FOSDataModel;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.schema.oaf.Result;

public class PrepareTest {

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
	void bipPrepareTest() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/createunresolvedentities/bip/bip.json")
			.getPath();

		PrepareBipFinder
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", sourcePath,
					"--outputPath", workingDir.toString() + "/work"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Result> tmp = sc
			.textFile(workingDir.toString() + "/work/bip")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		Assertions.assertEquals(86, tmp.count());

		String doi1 = "unresolved::10.0000/096020199389707::doi";

		Assertions.assertEquals(1, tmp.filter(r -> r.getId().equals(doi1)).count());
		Assertions.assertEquals(1, tmp.filter(r -> r.getId().equals(doi1)).collect().get(0).getInstance().size());
		Assertions
			.assertEquals(
				3, tmp.filter(r -> r.getId().equals(doi1)).collect().get(0).getInstance().get(0).getMeasures().size());
		Assertions
			.assertEquals(
				"6.34596412687e-09", tmp
					.filter(r -> r.getId().equals(doi1))
					.collect()
					.get(0)
					.getInstance()
					.get(0)
					.getMeasures()
					.stream()
					.filter(sl -> sl.getId().equals("influence"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"0.641151896994", tmp
					.filter(r -> r.getId().equals(doi1))
					.collect()
					.get(0)
					.getInstance()
					.get(0)
					.getMeasures()
					.stream()
					.filter(sl -> sl.getId().equals("popularity_alt"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());
		Assertions
			.assertEquals(
				"2.33375102921e-09", tmp
					.filter(r -> r.getId().equals(doi1))
					.collect()
					.get(0)
					.getInstance()
					.get(0)
					.getMeasures()
					.stream()
					.filter(sl -> sl.getId().equals("popularity"))
					.collect(Collectors.toList())
					.get(0)
					.getUnit()
					.get(0)
					.getValue());

	}

	@Test
	void getFOSFileTest() throws IOException, ClassNotFoundException {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/createunresolvedentities/fos/h2020_fos_sbs.csv")
			.getPath();
		final String outputPath = workingDir.toString() + "/fos.json";

		new GetFOSData()
			.doRewrite(
				sourcePath, outputPath, "eu.dnetlib.dhp.actionmanager.createunresolvedentities.model.FOSDataModel",
				'\t', fs);

		BufferedReader in = new BufferedReader(
			new InputStreamReader(fs.open(new org.apache.hadoop.fs.Path(outputPath))));

		String line;
		int count = 0;
		while ((line = in.readLine()) != null) {
			FOSDataModel fos = new ObjectMapper().readValue(line, FOSDataModel.class);

			System.out.println(new ObjectMapper().writeValueAsString(fos));
			count += 1;
		}

		assertEquals(38, count);

	}

	@Test
	void fosPrepareTest() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/createunresolvedentities/fos/fos.json")
			.getPath();

		PrepareFOSSparkJob
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", sourcePath,

					"-outputPath", workingDir.toString() + "/work"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Result> tmp = sc
			.textFile(workingDir.toString() + "/work/fos")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		String doi1 = "unresolved::10.3390/s18072310::doi";

		assertEquals(50, tmp.count());
		assertEquals(1, tmp.filter(row -> row.getId().equals(doi1)).count());
		assertTrue(
			tmp
				.filter(r -> r.getId().equals(doi1))
				.flatMap(r -> r.getSubject().iterator())
				.map(sbj -> sbj.getValue())
				.collect()
				.contains("engineering and technology"));

		assertTrue(
			tmp
				.filter(r -> r.getId().equals(doi1))
				.flatMap(r -> r.getSubject().iterator())
				.map(sbj -> sbj.getValue())
				.collect()
				.contains("nano-technology"));
		assertTrue(
			tmp
				.filter(r -> r.getId().equals(doi1))
				.flatMap(r -> r.getSubject().iterator())
				.map(sbj -> sbj.getValue())
				.collect()
				.contains("nanoscience & nanotechnology"));

		String doi = "unresolved::10.1111/1365-2656.12831::doi";
		assertEquals(1, tmp.filter(row -> row.getId().equals(doi)).count());
		assertTrue(
			tmp
				.filter(r -> r.getId().equals(doi))
				.flatMap(r -> r.getSubject().iterator())
				.map(sbj -> sbj.getValue())
				.collect()
				.contains("psychology and cognitive sciences"));

		assertTrue(
			tmp
				.filter(r -> r.getId().equals(doi))
				.flatMap(r -> r.getSubject().iterator())
				.map(sbj -> sbj.getValue())
				.collect()
				.contains("social sciences"));
		assertFalse(
			tmp
				.filter(r -> r.getId().equals(doi))
				.flatMap(r -> r.getSubject().iterator())
				.map(sbj -> sbj.getValue())
				.collect()
				.contains("NULL"));

	}

}
