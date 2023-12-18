
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
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.createunresolvedentities.model.FOSDataModel;
import eu.dnetlib.dhp.actionmanager.createunresolvedentities.model.SDGDataModel;
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

		assertEquals(20, tmp.count());
		assertEquals(1, tmp.filter(row -> row.getId().equals(doi1)).count());

		assertTrue(
			tmp
				.filter(r -> r.getId().equals(doi1))
				.flatMap(r -> r.getSubject().iterator())
				.map(sbj -> sbj.getValue())
				.collect()
				.contains("04 agricultural and veterinary sciences"));
		assertTrue(
			tmp
				.filter(r -> r.getId().equals(doi1))
				.flatMap(r -> r.getSubject().iterator())
				.map(sbj -> sbj.getValue())
				.collect()
				.contains("0404 agricultural biotechnology"));

		String doi = "unresolved::10.1007/s11164-020-04383-6::doi";
		assertEquals(1, tmp.filter(row -> row.getId().equals(doi)).count());
		assertTrue(
			tmp
				.filter(r -> r.getId().equals(doi))
				.flatMap(r -> r.getSubject().iterator())
				.map(sbj -> sbj.getValue())
				.collect()
				.contains("01 natural sciences"));

		assertTrue(
			tmp
				.filter(r -> r.getId().equals(doi))
				.flatMap(r -> r.getSubject().iterator())
				.map(sbj -> sbj.getValue())
				.collect()
				.contains("0104 chemical sciences"));
		assertTrue(
			tmp
				.filter(r -> r.getId().equals(doi))
				.flatMap(r -> r.getSubject().iterator())
				.map(sbj -> sbj.getValue())
				.collect()
				.contains("010402 general chemistry"));

	}

	@Test
	void fosPrepareTest2() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/createunresolvedentities/fos/fos_sbs_2.json")
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

		String doi1 = "unresolved::10.1016/j.revmed.2006.07.012::doi";

		assertEquals(13, tmp.count());
		assertEquals(1, tmp.filter(row -> row.getId().equals(doi1)).count());

		Result result = tmp
			.filter(r -> r.getId().equals(doi1))
			.first();

		result.getSubject().forEach(s -> System.out.println(s.getValue() + " trust = " + s.getDataInfo().getTrust()));
		Assertions.assertEquals(6, result.getSubject().size());

		assertTrue(
			result
				.getSubject()
				.stream()
				.anyMatch(
					s -> s.getValue().contains("03 medical and health sciences")
						&& s.getDataInfo().getTrust().equals("")));

		assertTrue(
			result
				.getSubject()
				.stream()
				.anyMatch(
					s -> s.getValue().contains("0302 clinical medicine") && s.getDataInfo().getTrust().equals("")));

		assertTrue(
			result
				.getSubject()
				.stream()
				.anyMatch(
					s -> s
						.getValue()
						.contains("030204 cardiovascular system & hematology")
						&& s.getDataInfo().getTrust().equals("0.5101401805877686")));
		assertTrue(
			result
				.getSubject()
				.stream()
				.anyMatch(
					s -> s
						.getValue()
						.contains("03020409 Hematology/Coagulopathies")
						&& s.getDataInfo().getTrust().equals("0.0546871414174914")));

	}

	@Test
	void sdgPrepareTest() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/actionmanager/createunresolvedentities/sdg/sdg.json")
			.getPath();

		PrepareSDGSparkJob
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", sourcePath,

					"-outputPath", workingDir.toString() + "/work"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Result> tmp = sc
			.textFile(workingDir.toString() + "/work/sdg")
			.map(item -> OBJECT_MAPPER.readValue(item, Result.class));

		String doi1 = "unresolved::10.1001/amaguidesnewsletters.2019.sepoct02::doi";

		assertEquals(32, tmp.count());
		assertEquals(1, tmp.filter(row -> row.getId().equals(doi1)).count());

		assertTrue(
			tmp
				.filter(r -> r.getId().equals(doi1))
				.flatMap(r -> r.getSubject().iterator())
				.map(sbj -> sbj.getValue())
				.collect()
				.contains("3. Good health"));
		assertTrue(
			tmp
				.filter(r -> r.getId().equals(doi1))
				.flatMap(r -> r.getSubject().iterator())
				.map(sbj -> sbj.getValue())
				.collect()
				.contains("8. Economic growth"));

		Assertions.assertEquals(32, tmp.filter(row -> row.getDataInfo() != null).count());

	}

}
