
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import eu.dnetlib.dhp.oa.graph.dump.community.ResultProject;
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

import eu.dnetlib.dhp.oa.graph.dump.community.SparkPrepareResultProject;

public class PrepareResultProjectJobTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory
		.getLogger(eu.dnetlib.dhp.oa.graph.dump.PrepareResultProjectJobTest.class);

	private static HashMap<String, String> map = new HashMap<>();

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(eu.dnetlib.dhp.oa.graph.dump.PrepareResultProjectJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(eu.dnetlib.dhp.oa.graph.dump.PrepareResultProjectJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(eu.dnetlib.dhp.oa.graph.dump.PrepareResultProjectJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void testNoMatch() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultProject/no_match")
			.getPath();

		SparkPrepareResultProject.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString() + "/preparedInfo",
			"-sourcePath", sourcePath
		});


		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<ResultProject> tmp = sc
			.textFile(workingDir.toString() + "/preparedInfo")
			.map(item -> OBJECT_MAPPER.readValue(item, ResultProject.class));

		org.apache.spark.sql.Dataset<ResultProject> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(ResultProject.class));

		Assertions.assertEquals(0, verificationDataset.count());

	}

	@Test
	public void testMatchOne() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultProject/match_one")
			.getPath();

		SparkPrepareResultProject.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString() + "/preparedInfo",
			"-sourcePath", sourcePath
		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<ResultProject> tmp = sc
			.textFile(workingDir.toString() + "/preparedInfo")
			.map(item -> OBJECT_MAPPER.readValue(item, ResultProject.class));

		org.apache.spark.sql.Dataset<ResultProject> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(ResultProject.class));

		Assertions.assertTrue(verificationDataset.count() == 1);

		Assertions
			.assertEquals(
				1,
				verificationDataset.filter("resultId = '50|dedup_wf_001::e4805d005bfab0cd39a1642cbf477fdb'").count());

		verificationDataset.createOrReplaceTempView("table");

		Dataset<Row> check = spark.sql("Select projList.provenance.provenance  " +
				"from table " +
				"lateral view explode (projectsList) pl as projList");

		Assertions.assertEquals(1, check.filter("provenance = 'sysimport:crosswalk:entityregistry'").count());

		verificationDataset.show(false);

	}

	@Test
	public void testMatch() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/resultProject/match")
			.getPath();

		SparkPrepareResultProject.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString() + "/preparedInfo",
			"-sourcePath", sourcePath
		});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<ResultProject> tmp = sc
			.textFile(workingDir.toString() + "/preparedInfo")
			.map(item -> OBJECT_MAPPER.readValue(item, ResultProject.class));

		org.apache.spark.sql.Dataset<ResultProject> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(ResultProject.class));

		Assertions.assertTrue(verificationDataset.count() == 2);

		Assertions
			.assertEquals(
				1,
				verificationDataset.filter("resultId = '50|dedup_wf_001::e4805d005bfab0cd39a1642cbf477fdb'").count());
		Assertions
			.assertEquals(
				1,
				verificationDataset.filter("resultId = '50|dedup_wf_001::51b88f272ba9c3bb181af64e70255a80'").count());

		verificationDataset.createOrReplaceTempView("dataset");

		String query = "select resultId, MyT.id project , MyT.title title, MyT.acronym acronym , MyT.provenance.provenance provenance "
			+ "from dataset "
			+ "lateral view explode(projectsList) p as MyT ";

		org.apache.spark.sql.Dataset<Row> resultExplodedProvenance = spark.sql(query);
		Assertions.assertEquals(3, resultExplodedProvenance.count());
		Assertions
			.assertEquals(
				2,
				resultExplodedProvenance
					.filter("resultId = '50|dedup_wf_001::e4805d005bfab0cd39a1642cbf477fdb'")
					.count());

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter("resultId = '50|dedup_wf_001::51b88f272ba9c3bb181af64e70255a80'")
					.count());

		Assertions
			.assertEquals(
				2,
				resultExplodedProvenance
					.filter("project = '40|aka_________::0f7d119de1f656b5763a16acf876fed6'")
					.count());

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter(
						"project = '40|aka_________::0f7d119de1f656b5763a16acf876fed6' and resultId = '50|dedup_wf_001::e4805d005bfab0cd39a1642cbf477fdb'")
					.count());

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter(
						"project = '40|aka_________::0f7d119de1f656b5763a16acf876fed6' and resultId = '50|dedup_wf_001::51b88f272ba9c3bb181af64e70255a80'")
					.count());

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter("project = '40|aka_________::03376222b28a3aebf2730ac514818d04'")
					.count());

		Assertions
			.assertEquals(
				1,
				resultExplodedProvenance
					.filter(
						"project = '40|aka_________::03376222b28a3aebf2730ac514818d04' and resultId = '50|dedup_wf_001::e4805d005bfab0cd39a1642cbf477fdb'")
					.count());

		Assertions.assertEquals(3, resultExplodedProvenance.filter("provenance = 'sysimport:crosswalk:entityregistry'").count());

	}

}
