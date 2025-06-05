
package eu.dnetlib.dhp.person;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.logging.Filter;

import javax.validation.constraints.AssertTrue;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
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

import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.utils.DHPUtils;

public class PersonPropagationJobTest {

	private static final Logger log = LoggerFactory.getLogger(PersonPropagationJobTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(PersonPropagationJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(PersonPropagationJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(PersonPropagationJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testPersonPropagation() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/person/graph")
			.getPath();

		spark
			.read()
			.json(sourcePath + "/publication")
			.write()
			.json(workingDir.toString() + "/graph/publication");
		spark
			.read()
			.json(sourcePath + "/dataset")
			.write()
			.json(workingDir.toString() + "/graph/dataset");
		spark
			.read()
			.json(sourcePath + "/software")
			.write()
			.json(workingDir.toString() + "/graph/software");
		spark
			.read()
			.json(sourcePath + "/otherresearchproduct")
			.write()
			.json(workingDir.toString() + "/graph/otherresearchproduct");
		spark
			.read()
			.json(sourcePath + "/person")
			.write()
			.json(workingDir.toString() + "/graph/person");
		spark
			.read()
			.json(sourcePath + "/relation")
			.write()
			.json(workingDir.toString() + "/graph/relation");
		SparkExtractPersonRelationsAndAddIndicators
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", workingDir.toString() + "/graph",
					"--outputPath", workingDir.toString() + "/working"
				});

		Dataset<Relation> relations = spark
			.read()
			.schema(Encoders.bean(Relation.class).schema())
			.json(workingDir.toString() + "/graph/relation")
			.as(Encoders.bean(Relation.class));

//		"50|doi_________::4892912a1a2c54a98fa85bb08afc2a32","0000-0001-8255-3618","0000-0001-8255-3618"
//		"50|doi_________::6ad85dd3c2dcc551912362b6e6c6c87a","0000-0001-8255-3619","0000-0001-8255-3618"
//		"50|doi_________::b2eae15cfe9b0d7f416b6dcfc84c09f9","0000-0001-8255-3620","0000-0001-8255-3618"
//		"50|doi_________::9c79b0fb92bec7740bf7edc47ca08445","0000-0001-8255-3621","0000-0001-8255-3618"
//		"50|doi_________::b2eae15cfe9b0d7f416b6dcfc84c09f8","0000-0001-8255-3622","0000-0001-8255-3618"
//
//		hasAuthored = 9
//		hasCoAuthor = 8

//50|doi_________::fa6db8629c4a8d13ec21e445b309d1c8",0000-0001-7605-9058,0000-0002-0447-8613,0000-0001-5491-7568
		// hasAuthored = 3 hasCoAuthor = 6

		Assertions.assertEquals(44, relations.count());
		Assertions
			.assertEquals(
				12,
				relations
					.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase("hasAuthored"))
					.count());
		relations
			.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase("hasAuthored"))
			.foreach((ForeachFunction<Relation>) r -> Assertions.assertTrue(r.getSource().startsWith("30|orcid")));
		relations
			.filter((FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase("hasAuthored"))
			.foreach((ForeachFunction<Relation>) r -> Assertions.assertTrue(r.getTarget().startsWith("50|")));
		Assertions
			.assertEquals(
				1,
				relations
					.filter(
						(FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase("hasAuthored")
							&& r.getTarget().equalsIgnoreCase("50|doi_________::4892912a1a2c54a98fa85bb08afc2a32"))
					.count());
		Assertions
			.assertEquals(
				2,
				relations
					.filter(
						(FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase("hasAuthored")
							&& r.getTarget().equalsIgnoreCase("50|doi_________::6ad85dd3c2dcc551912362b6e6c6c87a"))
					.count());

		Assertions
			.assertEquals(
				4,
				relations
					.filter(
						(FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase("hascoauthor") && r
							.getSource()
							.equalsIgnoreCase("30|orcid_______::" + DHPUtils.md5("0000-0001-8255-3618")))
					.count());
		Assertions
			.assertEquals(
				4,
				relations
					.filter(
						(FilterFunction<Relation>) r -> r.getRelClass().equalsIgnoreCase("hascoauthor") && r
							.getTarget()
							.equalsIgnoreCase("30|orcid_______::" + DHPUtils.md5("0000-0001-8255-3618")))
					.count());

		Dataset<Person> person = spark
			.read()
			.schema(Encoders.bean(Person.class).schema())
			.json(workingDir.toString() + "/graph/person")
			.as(Encoders.bean(Person.class));

		Assertions.assertEquals(8, person.count());
		Assertions
			.assertEquals(
				3,
				person
					.filter((FilterFunction<Person>) p -> p.getMeasures() != null && !p.getMeasures().isEmpty())
					.count());

		List<Measure> measures = person
			.filter(
				(FilterFunction<Person>) p -> p
					.getId()
					.equals("30|orcid_______::" + DHPUtils.md5("0000-0001-8255-3618")))
			.first()
			.getMeasures();
		measures.forEach(m -> {
			if (m.getId().equalsIgnoreCase("downloads"))
				Assertions.assertEquals("30", m.getUnit().get(0).getValue());
			else
				Assertions.assertEquals("9", m.getUnit().get(0).getValue());
		});

		measures = person
			.filter(
				(FilterFunction<Person>) p -> p
					.getId()
					.equals("30|orcid_______::" + DHPUtils.md5("0000-0001-8255-3619")))
			.first()
			.getMeasures();
		measures.forEach(m -> {
			if (m.getId().equalsIgnoreCase("downloads"))
				Assertions.assertEquals("10", m.getUnit().get(0).getValue());
			else
				Assertions.assertEquals("3", m.getUnit().get(0).getValue());
		});

		measures = person
			.filter(
				(FilterFunction<Person>) p -> p
					.getId()
					.equals("30|orcid_______::" + DHPUtils.md5("0000-0001-8255-3620")))
			.first()
			.getMeasures();
		measures.forEach(m -> {
			if (m.getId().equalsIgnoreCase("downloads"))
				Assertions.assertEquals("10", m.getUnit().get(0).getValue());
			else
				Assertions.assertEquals("3", m.getUnit().get(0).getValue());
		});

	}

}
