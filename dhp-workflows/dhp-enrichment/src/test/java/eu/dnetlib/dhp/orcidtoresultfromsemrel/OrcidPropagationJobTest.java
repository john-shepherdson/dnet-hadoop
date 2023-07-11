
package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class OrcidPropagationJobTest {

	private static final Logger log = LoggerFactory.getLogger(OrcidPropagationJobTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(OrcidPropagationJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(OrcidPropagationJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(OrcidPropagationJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void noUpdateTest() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/orcidtoresultfromsemrel/sample/noupdate")
			.getPath();
		final String possibleUpdatesPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/orcidtoresultfromsemrel/preparedInfo/mergedOrcidAssoc")
			.getPath();
		SparkOrcidToResultFromSemRelJob
			.main(
				new String[] {
					"-isTest", Boolean.TRUE.toString(),
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-hive_metastore_uris", "",
					"-saveGraph", "true",
					"-resultTableName", Dataset.class.getCanonicalName(),
					"-outputPath", workingDir.toString() + "/dataset",
					"-possibleUpdatesPath", possibleUpdatesPath
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		// tmp.map(s -> new Gson().toJson(s)).foreach(s -> System.out.println(s));

		Assertions.assertEquals(10, tmp.count());

		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));

		verificationDataset.createOrReplaceTempView("dataset");

		String query = "select id "
			+ "from dataset "
			+ "lateral view explode(author) a as MyT "
			+ "lateral view explode(MyT.pid) p as MyP "
			+ "where MyP.datainfo.inferenceprovenance = 'propagation'";

		Assertions.assertEquals(0, spark.sql(query).count());
	}

	@Test
	void oneUpdateTest() throws Exception {
		SparkOrcidToResultFromSemRelJob
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass()
						.getResource("/eu/dnetlib/dhp/orcidtoresultfromsemrel/sample/oneupdate")
						.getPath(),
					"-hive_metastore_uris",
					"",
					"-saveGraph",
					"true",
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath",
					workingDir.toString() + "/dataset",
					"-possibleUpdatesPath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/orcidtoresultfromsemrel/preparedInfo/mergedOrcidAssoc")
						.getPath()
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		// tmp.map(s -> new Gson().toJson(s)).foreach(s -> System.out.println(s));

		Assertions.assertEquals(10, tmp.count());

		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));

		verificationDataset.createOrReplaceTempView("dataset");

		String query = "select id, MyT.name name, MyT.surname surname, MyP.value pid, MyP.qualifier.classid pidType "
			+ "from dataset "
			+ "lateral view explode(author) a as MyT "
			+ "lateral view explode(MyT.pid) p as MyP "
			+ "where MyP.datainfo.inferenceprovenance = 'propagation'";

		org.apache.spark.sql.Dataset<Row> propagatedAuthors = spark.sql(query);

		Assertions.assertEquals(1, propagatedAuthors.count());

		Assertions
			.assertEquals(
				1,
				propagatedAuthors
					.filter(
						"id = '50|dedup_wf_001::95b033c0c3961f6a1cdcd41a99a9632e' "
							+ "and name = 'Vajinder' and surname = 'Kumar' and pidType = '" +

							ModelConstants.ORCID_PENDING + "'")
					.count());

		Assertions.assertEquals(1, propagatedAuthors.filter("pid = '0000-0002-8825-3517'").count());
	}

	@Test
	void twoUpdatesTest() throws Exception {
		SparkOrcidToResultFromSemRelJob
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-sourcePath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/orcidtoresultfromsemrel/sample/twoupdates")
						.getPath(),
					"-hive_metastore_uris",
					"",
					"-saveGraph",
					"true",
					"-resultTableName",
					"eu.dnetlib.dhp.schema.oaf.Dataset",
					"-outputPath",
					workingDir.toString() + "/dataset",
					"-possibleUpdatesPath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/orcidtoresultfromsemrel/preparedInfo/mergedOrcidAssoc")
						.getPath()
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Dataset> tmp = sc
			.textFile(workingDir.toString() + "/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(10, tmp.count());

		org.apache.spark.sql.Dataset<Dataset> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Dataset.class));

		verificationDataset.createOrReplaceTempView("dataset");

		String query = "select id, MyT.name name, MyT.surname surname, MyP.value pid, MyP.qualifier.classid pidType "
			+ "from dataset "
			+ "lateral view explode(author) a as MyT "
			+ "lateral view explode(MyT.pid) p as MyP "
			+ "where MyP.datainfo.inferenceprovenance = 'propagation'";

		org.apache.spark.sql.Dataset<Row> propagatedAuthors = spark.sql(query);

		Assertions.assertEquals(2, propagatedAuthors.count());

		Assertions
			.assertEquals(
				1, propagatedAuthors.filter("name = 'Marc' and surname = 'Schmidtmann'").count());
		Assertions
			.assertEquals(
				1, propagatedAuthors.filter("name = 'Ruediger' and surname = 'Beckhaus'").count());

		query = "select id, MyT.name name, MyT.surname surname, MyP.value pid ,MyP.qualifier.classid pidType "
			+ "from dataset "
			+ "lateral view explode(author) a as MyT "
			+ "lateral view explode(MyT.pid) p as MyP ";

		org.apache.spark.sql.Dataset<Row> authorsExplodedPids = spark.sql(query);

		Assertions
			.assertEquals(
				2, authorsExplodedPids.filter("name = 'Marc' and surname = 'Schmidtmann'").count());
		Assertions
			.assertEquals(
				1,
				authorsExplodedPids
					.filter(
						"name = 'Marc' and surname = 'Schmidtmann' and pidType = 'MAG Identifier'")
					.count());
	}
}
