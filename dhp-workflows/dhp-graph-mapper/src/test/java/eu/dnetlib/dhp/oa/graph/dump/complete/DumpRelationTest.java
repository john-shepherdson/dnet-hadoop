
package eu.dnetlib.dhp.oa.graph.dump.complete;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import java.util.HashMap;


import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
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

import eu.dnetlib.dhp.schema.dump.oaf.graph.Relation;

public class DumpRelationTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory
		.getLogger(DumpRelationTest.class);

	private static final HashMap<String, String> map = new HashMap<>();

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(DumpRelationTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(DumpRelationTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(DumpRelationTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	public void test1() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/relation/relation")
			.getPath();

		SparkDumpRelationJob.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString() + "/relation",
			"-sourcePath", sourcePath
		});


		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		org.apache.spark.sql.Dataset<Relation> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		verificationDataset.createOrReplaceTempView("table");

		verificationDataset
			.foreach((ForeachFunction<Relation>) r -> System.out.println(new ObjectMapper().writeValueAsString(r)));

		Dataset<Row> check = spark
			.sql(
				"SELECT reltype.name, source.id source, source.type stype, target.id target,target.type ttype, provenance.provenance "
					+
					"from table ");

		Assertions.assertEquals(22, check.filter("name = 'isProvidedBy'").count());
		Assertions
			.assertEquals(
				22, check
					.filter(
						"name = 'isProvidedBy' and stype = 'datasource' and ttype = 'organization' and " +
							"provenance = 'Harvested'")
					.count());

		Assertions.assertEquals(7, check.filter("name = 'isParticipant'").count());
		Assertions
			.assertEquals(
				7, check
					.filter(
						"name = 'isParticipant' and stype = 'organization' and ttype = 'project' " +
							"and provenance = 'Harvested'")
					.count());

		Assertions.assertEquals(1, check.filter("name = 'isAuthorInstitutionOf'").count());
		Assertions
			.assertEquals(
				1, check
					.filter(
						"name = 'isAuthorInstitutionOf' and stype = 'organization' and ttype = 'result' " +
							"and provenance = 'Inferred by OpenAIRE'")
					.count());
	}

	@Test
	public void test2() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/relation/relation_validated")
			.getPath();

		SparkDumpRelationJob.main(new String[] {
			"-isSparkSessionManaged", Boolean.FALSE.toString(),
			"-outputPath", workingDir.toString() + "/relation",
			"-sourcePath", sourcePath
		});


		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
			.textFile(workingDir.toString() + "/relation")
			.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		org.apache.spark.sql.Dataset<Relation> verificationDataset = spark
			.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		verificationDataset.createOrReplaceTempView("table");

		verificationDataset
			.foreach((ForeachFunction<Relation>) r -> System.out.println(new ObjectMapper().writeValueAsString(r)));

		Dataset<Row> check = spark
			.sql(
				"SELECT reltype.name, source.id source, source.type stype, target.id target,target.type ttype, provenance.provenance "
					+
					"from table ");

		Assertions.assertEquals(20, check.filter("name = 'isProvidedBy'").count());
		Assertions
			.assertEquals(
				20, check
					.filter(
						"name = 'isProvidedBy' and stype = 'datasource' and ttype = 'organization' and " +
							"provenance = 'Harvested'")
					.count());

		Assertions.assertEquals(7, check.filter("name = 'isParticipant'").count());
		Assertions
			.assertEquals(
				7, check
					.filter(
						"name = 'isParticipant' and stype = 'organization' and ttype = 'project' " +
							"and provenance = 'Harvested'")
					.count());

		Assertions.assertEquals(1, check.filter("name = 'isAuthorInstitutionOf'").count());
		Assertions
			.assertEquals(
				1, check
					.filter(
						"name = 'isAuthorInstitutionOf' and stype = 'organization' and ttype = 'result' " +
							"and provenance = 'Inferred by OpenAIRE'")
					.count());

		Assertions.assertEquals(2, check.filter("name = 'isProducedBy'").count());
		Assertions
			.assertEquals(
				2, check
					.filter(
						"name = 'isProducedBy' and stype = 'project' and ttype = 'result' " +
							"and provenance = 'Harvested' and validated = true " +
							"and validationDate = '2021-08-06'")
					.count());
	}

	@Test
	public void test3() throws Exception {//
		final String sourcePath = getClass()
				.getResource("/eu/dnetlib/dhp/oa/graph/dump/relation/relation")
				.getPath();

		SparkDumpRelationJob.main(new String[] {
				"-isSparkSessionManaged", Boolean.FALSE.toString(),
				"-outputPath", workingDir.toString() + "/relation",
				"-sourcePath", sourcePath,
				"-removeSet", "isParticipant"
		});


		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
				.textFile(workingDir.toString() + "/relation")
				.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		org.apache.spark.sql.Dataset<Relation> verificationDataset = spark
				.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		verificationDataset.createOrReplaceTempView("table");

		verificationDataset
				.foreach((ForeachFunction<Relation>) r -> System.out.println(new ObjectMapper().writeValueAsString(r)));

		Dataset<Row> check = spark
				.sql(
						"SELECT reltype.name, source.id source, source.type stype, target.id target,target.type ttype, provenance.provenance "
								+
								"from table ");

		Assertions.assertEquals(22, check.filter("name = 'isProvidedBy'").count());
		Assertions
				.assertEquals(
						22, check
								.filter(
										"name = 'isProvidedBy' and stype = 'datasource' and ttype = 'organization' and " +
												"provenance = 'Harvested'")
								.count());

		Assertions.assertEquals(0, check.filter("name = 'isParticipant'").count());


		Assertions.assertEquals(1, check.filter("name = 'isAuthorInstitutionOf'").count());
		Assertions
				.assertEquals(
						1, check
								.filter(
										"name = 'isAuthorInstitutionOf' and stype = 'organization' and ttype = 'result' " +
												"and provenance = 'Inferred by OpenAIRE'")
								.count());
	}

	@Test
	public void test4() throws Exception {//
		final String sourcePath = getClass()
				.getResource("/eu/dnetlib/dhp/oa/graph/dump/relation/relation")
				.getPath();

		SparkDumpRelationJob.main(new String[] {
				"-isSparkSessionManaged", Boolean.FALSE.toString(),
				"-outputPath", workingDir.toString() + "/relation",
				"-sourcePath", sourcePath,
				"-removeSet", "isParticipant;isAuthorInstitutionOf"
		});


		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Relation> tmp = sc
				.textFile(workingDir.toString() + "/relation")
				.map(item -> OBJECT_MAPPER.readValue(item, Relation.class));

		org.apache.spark.sql.Dataset<Relation> verificationDataset = spark
				.createDataset(tmp.rdd(), Encoders.bean(Relation.class));

		verificationDataset.createOrReplaceTempView("table");

		verificationDataset
				.foreach((ForeachFunction<Relation>) r -> System.out.println(new ObjectMapper().writeValueAsString(r)));

		Dataset<Row> check = spark
				.sql(
						"SELECT reltype.name, source.id source, source.type stype, target.id target,target.type ttype, provenance.provenance "
								+
								"from table ");

		Assertions.assertEquals(22, check.filter("name = 'isProvidedBy'").count());
		Assertions
				.assertEquals(
						22, check
								.filter(
										"name = 'isProvidedBy' and stype = 'datasource' and ttype = 'organization' and " +
												"provenance = 'Harvested'")
								.count());

		Assertions.assertEquals(0, check.filter("name = 'isParticipant'").count());


		Assertions.assertEquals(0, check.filter("name = 'isAuthorInstitutionOf'").count());

	}

}
