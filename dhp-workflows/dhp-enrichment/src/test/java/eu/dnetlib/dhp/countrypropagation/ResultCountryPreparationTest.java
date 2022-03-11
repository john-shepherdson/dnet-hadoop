
package eu.dnetlib.dhp.countrypropagation;

import static eu.dnetlib.dhp.PropagationConstant.isSparkSessionManaged;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Publication;

public class ResultCountryPreparationTest {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(ResultCountryPreparationTest.class.getSimpleName());

		SparkConf conf = new SparkConf();
		conf.setAppName(ResultCountryPreparationTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(ResultCountryPreparationTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testPrepareResultCountry() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/countrypropagation/graph/publication")
			.getPath();

		final String preparedInfoPath = getClass()
			.getResource("/eu/dnetlib/dhp/countrypropagation/datasourcecountry")
			.getPath();

		PrepareResultCountrySet
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--workingPath", workingDir.toString() + "/working",
					"--sourcePath", sourcePath,
					"--outputPath", workingDir.toString() + "/resultCountry",
					"--preparedInfoPath", preparedInfoPath,
					"--resultTableName", Publication.class.getCanonicalName()
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<ResultCountrySet> tmp = sc
			.textFile(workingDir.toString() + "/resultCountry")
			.map(item -> OBJECT_MAPPER.readValue(item, ResultCountrySet.class));

		Assertions.assertEquals(5, tmp.count());

		ResultCountrySet rc = tmp
			.filter(r -> r.getResultId().equals("50|06cdd3ff4700::49ec404cee4e1452808aabeaffbd3072"))
			.collect()
			.get(0);
		Assertions.assertEquals(1, rc.getCountrySet().size());
		Assertions.assertEquals("NL", rc.getCountrySet().get(0).getClassid());
		Assertions.assertEquals("Netherlands", rc.getCountrySet().get(0).getClassname());

		rc = tmp
			.filter(r -> r.getResultId().equals("50|07b5c0ccd4fe::e7f5459cc97865f2af6e3da964c1250b"))
			.collect()
			.get(0);
		Assertions.assertEquals(1, rc.getCountrySet().size());
		Assertions.assertEquals("NL", rc.getCountrySet().get(0).getClassid());
		Assertions.assertEquals("Netherlands", rc.getCountrySet().get(0).getClassname());

		rc = tmp
			.filter(r -> r.getResultId().equals("50|355e65625b88::e7d48a470b13bda61f7ebe3513e20cb6"))
			.collect()
			.get(0);
		Assertions.assertEquals(2, rc.getCountrySet().size());
		Assertions
			.assertTrue(
				rc
					.getCountrySet()
					.stream()
					.anyMatch(cs -> cs.getClassid().equals("IT") && cs.getClassname().equals("Italy")));
		Assertions
			.assertTrue(
				rc
					.getCountrySet()
					.stream()
					.anyMatch(cs -> cs.getClassid().equals("FR") && cs.getClassname().equals("France")));

		rc = tmp
			.filter(r -> r.getResultId().equals("50|355e65625b88::74009c567c81b4aa55c813db658734df"))
			.collect()
			.get(0);
		Assertions.assertEquals(2, rc.getCountrySet().size());
		Assertions
			.assertTrue(
				rc
					.getCountrySet()
					.stream()
					.anyMatch(cs -> cs.getClassid().equals("IT") && cs.getClassname().equals("Italy")));
		Assertions
			.assertTrue(
				rc
					.getCountrySet()
					.stream()
					.anyMatch(cs -> cs.getClassid().equals("NL") && cs.getClassname().equals("Netherlands")));

		rc = tmp
			.filter(r -> r.getResultId().equals("50|355e65625b88::54a1c76f520bb2c8da27d12e42891088"))
			.collect()
			.get(0);
		Assertions.assertEquals(2, rc.getCountrySet().size());
		Assertions
			.assertTrue(
				rc
					.getCountrySet()
					.stream()
					.anyMatch(cs -> cs.getClassid().equals("IT") && cs.getClassname().equals("Italy")));
		Assertions
			.assertTrue(
				rc
					.getCountrySet()
					.stream()
					.anyMatch(cs -> cs.getClassid().equals("FR") && cs.getClassname().equals("France")));

	}
}
