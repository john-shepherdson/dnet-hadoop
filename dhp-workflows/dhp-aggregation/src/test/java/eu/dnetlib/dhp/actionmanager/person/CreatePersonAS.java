
package eu.dnetlib.dhp.actionmanager.person;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.opencitations.CreateActionSetSparkJob;
import eu.dnetlib.dhp.actionmanager.personentity.ExtractPerson;
import eu.dnetlib.dhp.collection.orcid.model.Author;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Person;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.utils.DHPUtils;

public class CreatePersonAS {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;
	private static final Logger log = LoggerFactory
		.getLogger(CreatePersonAS.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files
			.createTempDirectory(CreatePersonAS.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(CreatePersonAS.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.codegen.wholeStage", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(CreatePersonAS.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testAuthors() throws Exception {

		String inputPath = getClass()
			.getResource(
				"/eu/dnetlib/dhp/actionmanager/person/")
			.getPath();

//		spark
//				.read()
//				.parquet(inputPath + "Authors")
//				.as(Encoders.bean(Author.class))
//						.filter((FilterFunction<Author>) a -> Optional.ofNullable(a.getOtherNames()).isPresent() &&
//								Optional.ofNullable(a.getBiography()).isPresent())
//								.write()
//										.mode(SaveMode.Overwrite)
//												.parquet(workingDir.toString() + "AuthorsSubset");

		ExtractPerson
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-inputPath",
					inputPath,
					"-outputPath",
					workingDir.toString() + "/actionSet1",
					"-workingDir",
					workingDir.toString() + "/working"
				});

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		JavaRDD<Relation> relations = sc
			.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
			.filter(v -> "eu.dnetlib.dhp.schema.oaf.Relation".equalsIgnoreCase(v._1().toString()))
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Relation) aa.getPayload()));
//
		JavaRDD<Person> people = sc
			.sequenceFile(workingDir.toString() + "/actionSet1", Text.class, Text.class)
			.filter(v -> "eu.dnetlib.dhp.schema.oaf.Person".equalsIgnoreCase(v._1().toString()))
			.map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
			.map(aa -> ((Person) aa.getPayload()));
//
		Assertions.assertEquals(7, people.count());
		Assertions
			.assertEquals(
				"Paulo",
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0002-3210-3034")))
					.first()
					.getGivenName());
		Assertions
			.assertEquals(
				"Tavares",
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0002-3210-3034")))
					.first()
					.getFamilyName());
		Assertions
			.assertEquals(
				4,
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0002-3210-3034")))
					.first()
					.getAlternativeNames()
					.size());
		Assertions
			.assertEquals(
				4,
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0002-3210-3034")))
					.first()
					.getPid()
					.size());
		Assertions
			.assertTrue(
				people
					.filter(
						p -> p.getPid().stream().anyMatch(id -> id.getValue().equalsIgnoreCase("0000-0002-3210-3034")))
					.first()
					.getPid()
					.stream()
					.anyMatch(
						p -> p.getSchema().equalsIgnoreCase("Scopus Author ID")
							&& p.getValue().equalsIgnoreCase("15119405200")));

		Assertions
			.assertEquals(
				16,
				relations
					.filter(r -> r.getRelClass().equalsIgnoreCase(ModelConstants.RESULT_PERSON_HASAUTHORED))
					.count());
		Assertions
			.assertEquals(
				14,
				relations
					.filter(r -> r.getRelClass().equalsIgnoreCase(ModelConstants.PERSON_PERSON_HASCOAUTHORED))
					.count());
		Assertions
			.assertEquals(
				3,
				relations
					.filter(
						r -> r.getSource().equalsIgnoreCase("30|orcid_______::" + DHPUtils.md5("0000-0001-6291-9619"))
							&& r.getRelClass().equalsIgnoreCase(ModelConstants.RESULT_PERSON_HASAUTHORED))
					.count());
		Assertions
			.assertEquals(
				2,
				relations
					.filter(
						r -> r.getSource().equalsIgnoreCase("30|orcid_______::" + DHPUtils.md5("0000-0001-6291-9619"))
							&& r.getRelClass().equalsIgnoreCase(ModelConstants.RESULT_PERSON_HASAUTHORED)
							&& r.getTarget().startsWith("50|doi"))
					.count());
		Assertions
			.assertEquals(
				1,
				relations
					.filter(
						r -> r.getSource().equalsIgnoreCase("30|orcid_______::" + DHPUtils.md5("0000-0001-6291-9619"))
							&& r.getRelClass().equalsIgnoreCase(ModelConstants.RESULT_PERSON_HASAUTHORED)
							&& r.getTarget().startsWith("50|arXiv"))
					.count());

		Assertions
			.assertEquals(
				1,
				relations
					.filter(
						r -> r.getSource().equalsIgnoreCase("30|orcid_______::" + DHPUtils.md5("0000-0001-6291-9619"))
							&& r.getRelClass().equalsIgnoreCase(ModelConstants.PERSON_PERSON_HASCOAUTHORED))
					.count());
		Assertions.assertEquals(33, relations.count());

	}

}
