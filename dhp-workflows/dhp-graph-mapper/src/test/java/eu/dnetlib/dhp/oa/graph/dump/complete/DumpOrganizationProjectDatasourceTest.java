
package eu.dnetlib.dhp.oa.graph.dump.complete;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import eu.dnetlib.dhp.oa.graph.dump.exceptions.NoAvailableEntityTypeException;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Project;

public class DumpOrganizationProjectDatasourceTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory
		.getLogger(DumpOrganizationProjectDatasourceTest.class);

	private static final HashMap<String, String> map = new HashMap<>();

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(DumpOrganizationProjectDatasourceTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(DumpOrganizationProjectDatasourceTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(DumpOrganizationProjectDatasourceTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void dumpOrganizationTest() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/complete/organization")
			.getPath();

		DumpGraphEntities dg = new DumpGraphEntities();

		dg.run(false, sourcePath, workingDir.toString() + "/dump", Organization.class, null);

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<eu.dnetlib.dhp.schema.dump.oaf.graph.Organization> tmp = sc
			.textFile(workingDir.toString() + "/dump")
			.map(item -> OBJECT_MAPPER.readValue(item, eu.dnetlib.dhp.schema.dump.oaf.graph.Organization.class));

		org.apache.spark.sql.Dataset<eu.dnetlib.dhp.schema.dump.oaf.graph.Organization> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.graph.Organization.class));

		Assertions.assertEquals(15, verificationDataset.count());

		verificationDataset
			.foreach(
				(ForeachFunction<eu.dnetlib.dhp.schema.dump.oaf.graph.Organization>) o -> System.out
					.println(OBJECT_MAPPER.writeValueAsString(o)));

	}

	@Test
	public void dumpProjectTest() throws NoAvailableEntityTypeException {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/complete/project")
			.getPath();

		DumpGraphEntities dg = new DumpGraphEntities();

		dg.run(false, sourcePath, workingDir.toString() + "/dump", Project.class, null);

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<eu.dnetlib.dhp.schema.dump.oaf.graph.Project> tmp = sc
			.textFile(workingDir.toString() + "/dump")
			.map(item -> OBJECT_MAPPER.readValue(item, eu.dnetlib.dhp.schema.dump.oaf.graph.Project.class));

		org.apache.spark.sql.Dataset<eu.dnetlib.dhp.schema.dump.oaf.graph.Project> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.graph.Project.class));

		Assertions.assertEquals(12, verificationDataset.count());

		verificationDataset
			.foreach(
				(ForeachFunction<eu.dnetlib.dhp.schema.dump.oaf.graph.Project>) o -> System.out
					.println(OBJECT_MAPPER.writeValueAsString(o)));

	}

	@Test
	public void dumpDatasourceTest() throws NoAvailableEntityTypeException {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/complete/datasource")
			.getPath();

		DumpGraphEntities dg = new DumpGraphEntities();

		dg.run(false, sourcePath, workingDir.toString() + "/dump", Datasource.class, null);

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<eu.dnetlib.dhp.schema.dump.oaf.graph.Datasource> tmp = sc
			.textFile(workingDir.toString() + "/dump")
			.map(item -> OBJECT_MAPPER.readValue(item, eu.dnetlib.dhp.schema.dump.oaf.graph.Datasource.class));

		org.apache.spark.sql.Dataset<eu.dnetlib.dhp.schema.dump.oaf.graph.Datasource> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.graph.Datasource.class));

		Assertions.assertEquals(5, verificationDataset.count());

		verificationDataset
			.foreach(
				(ForeachFunction<eu.dnetlib.dhp.schema.dump.oaf.graph.Datasource>) o -> System.out
					.println(OBJECT_MAPPER.writeValueAsString(o)));
	}

}
