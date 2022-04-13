
package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class PrepareStep2Test {

	private static final Logger log = LoggerFactory.getLogger(PrepareStep2Test.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(PrepareStep2Test.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(PrepareStep2Test.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());

		spark = SparkSession
			.builder()
			.appName(PrepareStep2Test.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testMatch() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/orcidtoresultfromsemrel/preparedInfo/resultSubset")
			.getPath();

		PrepareResultOrcidAssociationStep2
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-outputPath", workingDir.toString() + "/preparedInfo/mergedOrcidAssoc"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<ResultOrcidList> tmp = sc
			.textFile(workingDir.toString() + "/preparedInfo/mergedOrcidAssoc")
			.map(item -> OBJECT_MAPPER.readValue(item, ResultOrcidList.class));

		Assertions.assertEquals(1, tmp.count());

		Assertions
			.assertEquals(
				1,
				tmp
					.filter(rol -> rol.getResultId().equals("50|475c1990cbb2::46b9f15a3e887ccb154a696c4e7e4217"))
					.count());

		Assertions
			.assertEquals(
				2, tmp
					.filter(rol -> rol.getResultId().equals("50|475c1990cbb2::46b9f15a3e887ccb154a696c4e7e4217"))
					.collect()
					.get(0)
					.getAuthorList()
					.size());

		Assertions
			.assertTrue(
				tmp
					.filter(rol -> rol.getResultId().equals("50|475c1990cbb2::46b9f15a3e887ccb154a696c4e7e4217"))
					.collect()
					.get(0)
					.getAuthorList()
					.stream()
					.anyMatch(aa -> aa.getOrcid().equals("0000-0002-1234-5678")));
		Assertions
			.assertTrue(
				tmp
					.filter(rol -> rol.getResultId().equals("50|475c1990cbb2::46b9f15a3e887ccb154a696c4e7e4217"))
					.collect()
					.get(0)
					.getAuthorList()
					.stream()
					.anyMatch(aa -> aa.getOrcid().equals("0000-0002-5001-6911")));

	}

	@Test
	void matchTest() throws Exception {

		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/orcidtoresultfromsemrel/preparestep1")
			.getPath();

		PrepareResultOrcidAssociationStep1
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath", sourcePath,
					"-resultTableName", Publication.class.getCanonicalName(),
					"-outputPath", workingDir.toString() + "/preparedInfo",
					"-allowedsemrels", "IsSupplementedBy;IsSupplementTo",
					"-allowedpids", "orcid;orcid_pending"
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<ResultOrcidList> tmp = sc
			.textFile(workingDir.toString() + "/preparedInfo/publication")
			.map(item -> OBJECT_MAPPER.readValue(item, ResultOrcidList.class));

		Assertions.assertEquals(1, tmp.count());

		tmp.foreach(e -> System.out.println(OBJECT_MAPPER.writeValueAsString(e)));

		Assertions
			.assertEquals(
				1, tmp
					.filter(rol -> rol.getResultId().equals("50|475c1990cbb2::46b9f15a3e887ccb154a696c4e7e4217"))
					.count());
		Assertions
			.assertEquals(
				1, tmp
					.filter(rol -> rol.getResultId().equals("50|475c1990cbb2::46b9f15a3e887ccb154a696c4e7e4217"))
					.collect()
					.get(0)
					.getAuthorList()
					.size());
		Assertions
			.assertEquals(
				"0000-0002-5001-6911",
				tmp
					.filter(rol -> rol.getResultId().equals("50|475c1990cbb2::46b9f15a3e887ccb154a696c4e7e4217"))
					.collect()
					.get(0)
					.getAuthorList()
					.get(0)
					.getOrcid());
		Assertions
			.assertEquals(
				"Barbarić-Mikočević, Željka",
				tmp
					.filter(rol -> rol.getResultId().equals("50|475c1990cbb2::46b9f15a3e887ccb154a696c4e7e4217"))
					.collect()
					.get(0)
					.getAuthorList()
					.get(0)
					.getFullname());
		Assertions
			.assertEquals(
				"Željka",
				tmp
					.filter(rol -> rol.getResultId().equals("50|475c1990cbb2::46b9f15a3e887ccb154a696c4e7e4217"))
					.collect()
					.get(0)
					.getAuthorList()
					.get(0)
					.getName());
		Assertions
			.assertEquals(
				"Barbarić-Mikočević",
				tmp
					.filter(rol -> rol.getResultId().equals("50|475c1990cbb2::46b9f15a3e887ccb154a696c4e7e4217"))
					.collect()
					.get(0)
					.getAuthorList()
					.get(0)
					.getSurname());

		Assertions
			.assertEquals(
				7, sc
					.textFile(workingDir.toString() + "/preparedInfo/relationSubset")
					.map(item -> OBJECT_MAPPER.readValue(item, Relation.class))
					.count());

		Assertions
			.assertEquals(
				1, sc
					.textFile(workingDir.toString() + "/preparedInfo/resultSubset")
					.map(item -> OBJECT_MAPPER.readValue(item, Publication.class))
					.count());

	}

}
