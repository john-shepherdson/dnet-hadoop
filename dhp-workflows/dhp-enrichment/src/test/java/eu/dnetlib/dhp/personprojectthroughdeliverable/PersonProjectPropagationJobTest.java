
package eu.dnetlib.dhp.personprojectthroughdeliverable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Filter;

import org.apache.commons.io.FileUtils;
import org.apache.neethi.Assertion;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
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

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.DHPUtils;

public class PersonProjectPropagationJobTest {

	private static final Logger log = LoggerFactory.getLogger(PersonProjectPropagationJobTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(PersonProjectPropagationJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(PersonProjectPropagationJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(PersonProjectPropagationJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	/**
	 * One of the relations in the possible updates is already linked to the project in the graph. All the others are
	 * not. There will be 9 new associations leading to 18 new relations
	 *
	 * @throws Exception
	 */
	@Test
	void projectAuthorRelationTestDeliverableOnly() throws Exception {

		SparkAuthorProjectRelationExtraction
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-workingDir", workingDir.toString(),
					"-sourcePath", getClass()
						.getResource(
							"/eu/dnetlib/dhp/person/projectrelsextraction/graph")
						.getPath(),
					"-classCodes", "0034"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		Dataset<Relation> relations = spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(workingDir.toString() + "/relation")
			.as(Encoders.bean(Relation.class));

		Assertions.assertEquals(2, relations.count());
		relations
			.foreach(
				(ForeachFunction<Relation>) relation -> Assertions
					.assertEquals("40|aka_________::4892912a1a2c54a98fa85bb08afc2a32", relation.getTarget()));
		Assertions
			.assertTrue(
				relations
					.filter(
						(FilterFunction<Relation>) r -> r
							.getSource()
							.equalsIgnoreCase("30|orcid_______::bc79e7b6b0e339357634105055d5f29c"))
					.count() > 0);
		Assertions
			.assertTrue(
				relations
					.filter(
						(FilterFunction<Relation>) r -> r
							.getSource()
							.equalsIgnoreCase("30|orcid_______::61fea345f34b5166d4afbe9e98fcbe4f"))
					.count() > 0);
		relations
			.foreach(
				(ForeachFunction<Relation>) relation -> Assertions
					.assertEquals("projectPerson", relation.getRelType()));
		relations
			.foreach(
				(ForeachFunction<Relation>) relation -> Assertions
					.assertEquals("participation", relation.getSubRelType()));
		relations
			.foreach(
				(ForeachFunction<Relation>) relation -> Assertions
					.assertEquals("participatesToProject", relation.getRelClass()));
		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

	}

	@Test
	void projectAuthorRelationTest() throws Exception {

		SparkAuthorProjectRelationExtraction
			.main(
				new String[] {

					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-workingDir", workingDir.toString(),
					"-sourcePath", getClass()
						.getResource(
							"/eu/dnetlib/dhp/person/projectrelsextraction/graph")
						.getPath(),
					"-classCodes", "0034;0017"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		Dataset<Relation> relations = spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(workingDir.toString() + "/relation")
			.as(Encoders.bean(Relation.class));

		Assertions.assertEquals(3, relations.count());
		Assertions
			.assertEquals(
				2,
				relations
					.filter(
						(FilterFunction<Relation>) relation -> relation
							.getTarget()
							.equalsIgnoreCase("40|aka_________::4892912a1a2c54a98fa85bb08afc2a32"))
					.count());
		Assertions
			.assertEquals(
				1,
				relations
					.filter(
						(FilterFunction<Relation>) relation -> relation
							.getTarget()
							.equalsIgnoreCase("40|aka_________::08271906a58b12101a2413c4eeaffe98"))
					.count());
		Assertions
			.assertTrue(
				relations
					.filter(
						(FilterFunction<Relation>) r -> r
							.getSource()
							.equalsIgnoreCase("30|orcid_______::bc79e7b6b0e339357634105055d5f29c"))
					.count() > 0);
		Assertions
			.assertTrue(
				relations
					.filter(
						(FilterFunction<Relation>) r -> r
							.getSource()
							.equalsIgnoreCase("30|orcid_______::61fea345f34b5166d4afbe9e98fcbe4f"))
					.count() > 0);
		relations
			.foreach(
				(ForeachFunction<Relation>) relation -> Assertions
					.assertEquals("projectPerson", relation.getRelType()));
		relations
			.foreach(
				(ForeachFunction<Relation>) relation -> Assertions
					.assertEquals("participation", relation.getSubRelType()));
		relations
			.foreach(
				(ForeachFunction<Relation>) relation -> Assertions
					.assertEquals("participatesToProject", relation.getRelClass()));
		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

	}
}
