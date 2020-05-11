
package eu.dnetlib.dhp.projecttoresult;

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

import eu.dnetlib.dhp.schema.oaf.Relation;

public class ProjectPropagationJobTest {

	private static final Logger log = LoggerFactory.getLogger(ProjectPropagationJobTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final ClassLoader cl = ProjectPropagationJobTest.class.getClassLoader();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(ProjectPropagationJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(ProjectPropagationJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(ProjectPropagationJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	/**
	 * There are no new relations to be added. All the possible relations have already been linked with the project in
	 * the graph
	 *
	 * @throws Exception
	 */
	@Test
	public void NoUpdateTest() throws Exception {

		SparkResultToProjectThroughSemRelJob
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					// "-sourcePath",
					// getClass().getResource("/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/sample/relation").getPath(),
					"-hive_metastore_uris",
					"",
					"-saveGraph",
					"true",
					"-outputPath",
					workingDir.toString() + "/relation",
					"-potentialUpdatePath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/projecttoresult/preparedInfo/noupdates/potentialUpdates")
						.getPath(),
					"-alreadyLinkedPath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/projecttoresult/preparedInfo/alreadyLinked")
						.getPath(),
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		Assertions.assertEquals(0, tmp.count());
	}

	/**
	 * All the possible updates will produce a new relation. No relations are already linked in the grpha
	 *
	 * @throws Exception
	 */
	@Test
	public void UpdateTenTest() throws Exception {
		SparkResultToProjectThroughSemRelJob
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					// "-sourcePath",
					// getClass().getResource("/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/sample/relation").getPath(),
					"-hive_metastore_uris",
					"",
					"-saveGraph",
					"true",
					"-outputPath",
					workingDir.toString() + "/relation",
					"-potentialUpdatePath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/projecttoresult/preparedInfo/tenupdates/potentialUpdates")
						.getPath(),
					"-alreadyLinkedPath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/projecttoresult/preparedInfo/alreadyLinked")
						.getPath(),
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		// got 20 new relations because "produces" and "isProducedBy" are added
		Assertions.assertEquals(10, tmp.count());

		Dataset<Relation> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		Assertions.assertEquals(5, verificationDs.filter("relClass = 'produces'").count());
		Assertions.assertEquals(5, verificationDs.filter("relClass = 'isProducedBy'").count());

		Assertions
			.assertEquals(
				5,
				verificationDs
					.filter(
						r -> r.getSource().substring(0, 2).equals("50")
							&& r.getTarget().substring(0, 2).equals("40")
							&& r.getRelClass().equals("isProducedBy"))
					.count());
		Assertions
			.assertEquals(
				5,
				verificationDs
					.filter(
						r -> r.getSource().substring(0, 2).equals("40")
							&& r.getTarget().substring(0, 2).equals("50")
							&& r.getRelClass().equals("produces"))
					.count());

		verificationDs.createOrReplaceTempView("temporary");

		Assertions
			.assertEquals(
				10,
				spark
					.sql(
						"Select * from temporary where datainfo.inferenceprovenance = 'propagation'")
					.count());
	}

	/**
	 * One of the relations in the possible updates is already linked to the project in the graph. All the others are
	 * not. There will be 9 new associations leading to 18 new relations
	 *
	 * @throws Exception
	 */
	@Test
	public void UpdateMixTest() throws Exception {
		SparkResultToProjectThroughSemRelJob
			.main(
				new String[] {
					"-isTest",
					Boolean.TRUE.toString(),
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					// "-sourcePath",
					// getClass().getResource("/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/sample/relation").getPath(),
					"-hive_metastore_uris",
					"",
					"-saveGraph",
					"true",
					"-outputPath",
					workingDir.toString() + "/relation",
					"-potentialUpdatePath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/projecttoresult/preparedInfo/updatesmixed/potentialUpdates")
						.getPath(),
					"-alreadyLinkedPath",
					getClass()
						.getResource(
							"/eu/dnetlib/dhp/projecttoresult/preparedInfo/alreadyLinked")
						.getPath(),
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		// JavaRDD<Relation> tmp = sc.textFile("/tmp/relation")
		// .map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		// got 20 new relations because "produces" and "isProducedBy" are added
		Assertions.assertEquals(8, tmp.count());

		Dataset<Relation> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		Assertions.assertEquals(4, verificationDs.filter("relClass = 'produces'").count());
		Assertions.assertEquals(4, verificationDs.filter("relClass = 'isProducedBy'").count());

		Assertions
			.assertEquals(
				4,
				verificationDs
					.filter(
						r -> r.getSource().substring(0, 2).equals("50")
							&& r.getTarget().substring(0, 2).equals("40")
							&& r.getRelClass().equals("isProducedBy"))
					.count());
		Assertions
			.assertEquals(
				4,
				verificationDs
					.filter(
						r -> r.getSource().substring(0, 2).equals("40")
							&& r.getTarget().substring(0, 2).equals("50")
							&& r.getRelClass().equals("produces"))
					.count());

		verificationDs.createOrReplaceTempView("temporary");

		Assertions
			.assertEquals(
				8,
				spark
					.sql(
						"Select * from temporary where datainfo.inferenceprovenance = 'propagation'")
					.count());
	}
}
