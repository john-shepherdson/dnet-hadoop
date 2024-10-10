
package eu.dnetlib.dhp.oa.graph;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.oa.graph.hive.GraphHiveImporterJob;
import eu.dnetlib.dhp.schema.common.ModelSupport;

public class GraphHiveImporterJobTest {

	private static final Logger log = LoggerFactory.getLogger(GraphHiveImporterJobTest.class);

	public static final String JDBC_DERBY_TEMPLATE = "jdbc:derby:;databaseName=%s/junit_metastore_db;create=true";

	private static SparkSession spark;

	private static Path workingDir;

	private static String dbName;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(GraphHiveImporterJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		dbName = RandomStringUtils.randomAlphabetic(5);
		log.info("using DB name {}", "test_" + dbName);

		SparkConf conf = new SparkConf();
		conf.setAppName(GraphHiveImporterJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());
		conf
			.set(
				"javax.jdo.option.ConnectionURL",
				String.format(JDBC_DERBY_TEMPLATE, workingDir.resolve("warehouse")));

		spark = SparkSession
			.builder()
			.appName(GraphHiveImporterJobTest.class.getSimpleName())
			.config(conf)
			.enableHiveSupport()
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testImportGraphAsHiveDB() throws Exception {

		GraphHiveImporterJob
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--inputPath", getClass().getResource("/eu/dnetlib/dhp/oa/graph/sample").getPath(),
					"--hiveMetastoreUris", "",
					"--hiveDbName", dbName
				});

		ModelSupport.oafTypes
			.forEach(
				(name, clazz) -> {
					long count = spark.read().table(dbName + "." + name).count();
					int expected = name.equals("relation") ? 100 : 10;

					Assertions
						.assertEquals(
							expected, count, String.format("%s should be %s", name, expected));
				});
	}
}
