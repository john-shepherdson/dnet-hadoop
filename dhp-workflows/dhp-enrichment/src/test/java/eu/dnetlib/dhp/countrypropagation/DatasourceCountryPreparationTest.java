
package eu.dnetlib.dhp.countrypropagation;

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

public class DatasourceCountryPreparationTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(DatasourceCountryPreparationTest.class.getSimpleName());

		SparkConf conf = new SparkConf();
		conf.setAppName(DatasourceCountryPreparationTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(DatasourceCountryPreparationTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testPrepareDatasourceCountry() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/countrypropagation/graph")
			.getPath();

		PrepareDatasourceCountryAssociation
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", sourcePath,
					"--outputPath", workingDir.toString() + "/datasourceCountry",
					"--allowedtypes", "pubsrepository::institutional",
					"--whitelist",
					"10|openaire____::3795d6478e30e2c9f787d427ff160944;10|opendoar____::16e6a3326dd7d868cbc926602a61e4d0;10|eurocrisdris::fe4903425d9040f680d8610d9079ea14;10|openaire____::5b76240cc27a58c6f7ceef7d8c36660e;10|openaire____::172bbccecf8fca44ab6a6653e84cb92a;10|openaire____::149c6590f8a06b46314eed77bfca693f;10|eurocrisdris::a6026877c1a174d60f81fd71f62df1c1;10|openaire____::4692342f0992d91f9e705c26959f09e0;10|openaire____::8d529dbb05ec0284662b391789e8ae2a;10|openaire____::345c9d171ef3c5d706d08041d506428c;10|opendoar____::1c1d4df596d01da60385f0bb17a4a9e0;10|opendoar____::7a614fd06c325499f1680b9896beedeb;10|opendoar____::1ee3dfcd8a0645a25a35977997223d22;10|opendoar____::d296c101daa88a51f6ca8cfc1ac79b50;10|opendoar____::798ed7d4ee7138d49b8828958048130a;10|openaire____::c9d2209ecc4d45ba7b4ca7597acb88a2;10|eurocrisdris::c49e0fe4b9ba7b7fab717d1f0f0a674d;10|eurocrisdris::9ae43d14471c4b33661fedda6f06b539;10|eurocrisdris::432ca599953ff50cd4eeffe22faf3e48"
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<DatasourceCountry> tmp = sc
			.textFile(workingDir.toString() + "/datasourceCountry")
			.map(item -> OBJECT_MAPPER.readValue(item, DatasourceCountry.class));

		Assertions.assertEquals(3, tmp.count());
		Assertions
			.assertEquals(
				1, tmp
					.filter(
						dsc -> dsc
							.getDataSourceId()
							.equals("10|eurocrisdris::fe4903425d9040f680d8610d9079ea14"))
					.count());
		Assertions
			.assertEquals(
				1, tmp
					.filter(
						dsc -> dsc
							.getDataSourceId()
							.equals("10|opendoar____::f0dd4a99fba6075a9494772b58f95280"))
					.count());
		Assertions
			.assertEquals(
				1, tmp
					.filter(
						dsc -> dsc
							.getDataSourceId()
							.equals("10|eurocrisdris::9ae43d14471c4b33661fedda6f06b539"))
					.count());

		Assertions
			.assertEquals(
				"NL", tmp
					.filter(
						dsc -> dsc
							.getDataSourceId()
							.equals("10|eurocrisdris::fe4903425d9040f680d8610d9079ea14"))
					.collect()
					.get(0)
					.getCountry()
					.getClassid());
		Assertions
			.assertEquals(
				"Netherlands", tmp
					.filter(
						dsc -> dsc
							.getDataSourceId()
							.equals("10|eurocrisdris::fe4903425d9040f680d8610d9079ea14"))
					.collect()
					.get(0)
					.getCountry()
					.getClassname());

		Assertions
			.assertEquals(
				"IT", tmp
					.filter(
						dsc -> dsc
							.getDataSourceId()
							.equals("10|opendoar____::f0dd4a99fba6075a9494772b58f95280"))
					.collect()
					.get(0)
					.getCountry()
					.getClassid());
		Assertions
			.assertEquals(
				"Italy", tmp
					.filter(
						dsc -> dsc
							.getDataSourceId()
							.equals("10|opendoar____::f0dd4a99fba6075a9494772b58f95280"))
					.collect()
					.get(0)
					.getCountry()
					.getClassname());

		Assertions
			.assertEquals(
				"FR", tmp
					.filter(
						dsc -> dsc
							.getDataSourceId()
							.equals("10|eurocrisdris::9ae43d14471c4b33661fedda6f06b539"))
					.collect()
					.get(0)
					.getCountry()
					.getClassid());
		Assertions
			.assertEquals(
				"France", tmp
					.filter(
						dsc -> dsc
							.getDataSourceId()
							.equals("10|eurocrisdris::9ae43d14471c4b33661fedda6f06b539"))
					.collect()
					.get(0)
					.getCountry()
					.getClassname());

		tmp.foreach(e -> System.out.println(OBJECT_MAPPER.writeValueAsString(e)));

	}
}
