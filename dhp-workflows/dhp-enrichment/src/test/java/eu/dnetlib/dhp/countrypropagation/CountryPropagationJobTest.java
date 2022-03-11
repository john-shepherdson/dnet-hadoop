
package eu.dnetlib.dhp.countrypropagation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Country;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Software;
import scala.Tuple2;

public class CountryPropagationJobTest {

	private static final Logger log = LoggerFactory.getLogger(CountryPropagationJobTest.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(CountryPropagationJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(CountryPropagationJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(CountryPropagationJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testCountryPropagationSoftware() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/countrypropagation/graph/software")
			.getPath();
		final String preparedInfoPath = getClass()
			.getResource("/eu/dnetlib/dhp/countrypropagation/preparedInfo/software")
			.getPath();
		SparkCountryPropagationJob
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", sourcePath,
					"-resultTableName", Software.class.getCanonicalName(),
					"-outputPath", workingDir.toString() + "/software",
					"-preparedInfoPath", preparedInfoPath
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Software> tmp = sc
			.textFile(workingDir.toString() + "/software")
			.map(item -> OBJECT_MAPPER.readValue(item, Software.class));

		Assertions.assertEquals(10, tmp.count());

		Dataset<Software> verificationDs = spark.createDataset(tmp.rdd(), Encoders.bean(Software.class));

		Assertions.assertEquals(6, verificationDs.filter("size(country) > 0").count());
		Assertions.assertEquals(3, verificationDs.filter("size(country) = 1").count());
		Assertions.assertEquals(3, verificationDs.filter("size(country) = 2").count());
		Assertions.assertEquals(0, verificationDs.filter("size(country) > 2").count());

		Dataset<String> countryExploded = verificationDs
			.flatMap(
				(FlatMapFunction<Software, Country>) row -> row.getCountry().iterator(), Encoders.bean(Country.class))
			.map((MapFunction<Country, String>) Qualifier::getClassid, Encoders.STRING());

		Assertions.assertEquals(9, countryExploded.count());

		Assertions.assertEquals(1, countryExploded.filter("value = 'FR'").count());
		Assertions.assertEquals(1, countryExploded.filter("value = 'TR'").count());
		Assertions.assertEquals(2, countryExploded.filter("value = 'IT'").count());
		Assertions.assertEquals(1, countryExploded.filter("value = 'US'").count());
		Assertions.assertEquals(1, countryExploded.filter("value = 'MX'").count());
		Assertions.assertEquals(1, countryExploded.filter("value = 'CH'").count());
		Assertions.assertEquals(2, countryExploded.filter("value = 'JP'").count());

		Dataset<Tuple2<String, String>> countryExplodedWithCountryclassid = verificationDs
			.flatMap((FlatMapFunction<Software, Tuple2<String, String>>) row -> {
				List<Tuple2<String, String>> prova = new ArrayList<>();
				List<Country> countryList = row.getCountry();
				countryList
					.forEach(
						c -> prova
							.add(
								new Tuple2<>(
									row.getId(), c.getClassid())));
				return prova.iterator();
			}, Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

		Assertions.assertEquals(9, countryExplodedWithCountryclassid.count());

		countryExplodedWithCountryclassid.show(false);
		Assertions
			.assertEquals(
				1,
				countryExplodedWithCountryclassid
					.filter(
						"_1 = '50|od______1582::6e7a9b21a2feef45673890432af34244' and _2 = 'FR' ")
					.count());
		Assertions
			.assertEquals(
				1,
				countryExplodedWithCountryclassid
					.filter(
						"_1 = '50|dedup_wf_001::40ea2f24181f6ae77b866ebcbffba523' and _2 = 'TR' ")
					.count());
		Assertions
			.assertEquals(
				2,
				countryExplodedWithCountryclassid
					.filter(
						"_1 = '50|od______1106::2b7ca9726230be8e862be224fd463ac4' and (_2 = 'IT' or _2 = 'MX') ")
					.count());
		Assertions
			.assertEquals(
				2,
				countryExplodedWithCountryclassid
					.filter(
						"_1 = '50|od_______935::46a0ad9964171c3dd13373f5427b9a1c' and (_2 = 'IT' or _2 = 'US') ")
					.count());
		Assertions
			.assertEquals(
				1,
				countryExplodedWithCountryclassid
					.filter(
						"_1 = '50|dedup_wf_001::b67bc915603fc01e445f2b5888ba7218' and _2 = 'JP'")
					.count());
		Assertions
			.assertEquals(
				2,
				countryExplodedWithCountryclassid
					.filter(
						"_1 = '50|od_______109::f375befa62a741e9250e55bcfa88f9a6' and (_2 = 'CH' or _2 = 'JP') ")
					.count());

		Dataset<Tuple2<String, String>> countryExplodedWithCountryclassname = verificationDs
			.flatMap(
				(FlatMapFunction<Software, Tuple2<String, String>>) row -> {
					List<Tuple2<String, String>> prova = new ArrayList<>();
					List<Country> countryList = row.getCountry();
					countryList
						.forEach(
							c -> prova
								.add(
									new Tuple2<>(
										row.getId(),
										c.getClassname())));
					return prova.iterator();
				},
				Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

		countryExplodedWithCountryclassname.show(false);
		Assertions
			.assertEquals(
				1,
				countryExplodedWithCountryclassname
					.filter(
						"_1 = '50|od______1582::6e7a9b21a2feef45673890432af34244' and _2 = 'France' ")
					.count());
		Assertions
			.assertEquals(
				1,
				countryExplodedWithCountryclassname
					.filter(
						"_1 = '50|dedup_wf_001::40ea2f24181f6ae77b866ebcbffba523' and _2 = 'Turkey' ")
					.count());
		Assertions
			.assertEquals(
				2,
				countryExplodedWithCountryclassname
					.filter(
						"_1 = '50|od______1106::2b7ca9726230be8e862be224fd463ac4' and (_2 = 'Italy' or _2 = 'Mexico') ")
					.count());
		Assertions
			.assertEquals(
				2,
				countryExplodedWithCountryclassname
					.filter(
						"_1 = '50|od_______935::46a0ad9964171c3dd13373f5427b9a1c' and (_2 = 'Italy' or _2 = 'United States') ")
					.count());
		Assertions
			.assertEquals(
				1,
				countryExplodedWithCountryclassname
					.filter(
						"_1 = '50|dedup_wf_001::b67bc915603fc01e445f2b5888ba7218' and _2 = 'Japan' ")
					.count());
		Assertions
			.assertEquals(
				2,
				countryExplodedWithCountryclassname
					.filter(
						"_1 = '50|od_______109::f375befa62a741e9250e55bcfa88f9a6' and (_2 = 'Switzerland' or _2 = 'Japan') ")
					.count());

		Dataset<Tuple2<String, String>> countryExplodedWithCountryProvenance = verificationDs
			.flatMap(
				(FlatMapFunction<Software, Tuple2<String, String>>) row -> {
					List<Tuple2<String, String>> prova = new ArrayList<>();
					List<Country> countryList = row.getCountry();
					countryList
						.forEach(
							c -> prova
								.add(
									new Tuple2<>(
										row.getId(),
										c
											.getDataInfo()
											.getInferenceprovenance())));
					return prova.iterator();
				},
				Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

		Assertions
			.assertEquals(
				7, countryExplodedWithCountryProvenance.filter("_2 = 'propagation'").count());
	}

	@Test
	void testCountryPropagationPublication() throws Exception {
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/countrypropagation/graph/publication")
			.getPath();
		final String preparedInfoPath = getClass()
			.getResource("/eu/dnetlib/dhp/countrypropagation/preparedInfo/publication")
			.getPath();
		SparkCountryPropagationJob
			.main(
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--sourcePath", sourcePath,
					"-resultTableName", Publication.class.getCanonicalName(),
					"-outputPath", workingDir.toString() + "/publication",
					"-preparedInfoPath", preparedInfoPath
				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Publication> tmp = sc
			.textFile(workingDir.toString() + "/publication")
			.map(item -> OBJECT_MAPPER.readValue(item, Publication.class));

		Assertions.assertEquals(12, tmp.count());

		Assertions.assertEquals(5, tmp.filter(r -> r.getCountry().size() > 0).count());

		tmp
			.foreach(
				r -> r.getCountry().stream().forEach(c -> Assertions.assertEquals("dnet:countries", c.getSchemeid())));
		tmp
			.foreach(
				r -> r
					.getCountry()
					.stream()
					.forEach(c -> Assertions.assertEquals("dnet:countries", c.getSchemename())));
		tmp
			.foreach(
				r -> r
					.getCountry()
					.stream()
					.forEach(c -> Assertions.assertFalse(c.getDataInfo().getDeletedbyinference())));
		tmp.foreach(r -> r.getCountry().stream().forEach(c -> Assertions.assertFalse(c.getDataInfo().getInvisible())));
		tmp.foreach(r -> r.getCountry().stream().forEach(c -> Assertions.assertTrue(c.getDataInfo().getInferred())));
		tmp
			.foreach(
				r -> r.getCountry().stream().forEach(c -> Assertions.assertEquals("0.85", c.getDataInfo().getTrust())));
		tmp
			.foreach(
				r -> r
					.getCountry()
					.stream()
					.forEach(c -> Assertions.assertEquals("propagation", c.getDataInfo().getInferenceprovenance())));
		tmp
			.foreach(
				r -> r
					.getCountry()
					.stream()
					.forEach(
						c -> Assertions
							.assertEquals("country:instrepos", c.getDataInfo().getProvenanceaction().getClassid())));
		tmp
			.foreach(
				r -> r
					.getCountry()
					.stream()
					.forEach(
						c -> Assertions
							.assertEquals(
								"dnet:provenanceActions", c.getDataInfo().getProvenanceaction().getSchemeid())));
		tmp
			.foreach(
				r -> r
					.getCountry()
					.stream()
					.forEach(
						c -> Assertions
							.assertEquals(
								"dnet:provenanceActions", c.getDataInfo().getProvenanceaction().getSchemename())));

		List<Country> countries = tmp
			.filter(r -> r.getId().equals("50|06cdd3ff4700::49ec404cee4e1452808aabeaffbd3072"))
			.collect()
			.get(0)
			.getCountry();
		Assertions.assertEquals(1, countries.size());
		Assertions.assertEquals("NL", countries.get(0).getClassid());
		Assertions.assertEquals("Netherlands", countries.get(0).getClassname());

		countries = tmp
			.filter(r -> r.getId().equals("50|07b5c0ccd4fe::e7f5459cc97865f2af6e3da964c1250b"))
			.collect()
			.get(0)
			.getCountry();
		Assertions.assertEquals(1, countries.size());
		Assertions.assertEquals("NL", countries.get(0).getClassid());
		Assertions.assertEquals("Netherlands", countries.get(0).getClassname());

		countries = tmp
			.filter(r -> r.getId().equals("50|355e65625b88::e7d48a470b13bda61f7ebe3513e20cb6"))
			.collect()
			.get(0)
			.getCountry();
		Assertions.assertEquals(2, countries.size());
		Assertions
			.assertTrue(
				countries.stream().anyMatch(cs -> cs.getClassid().equals("IT") && cs.getClassname().equals("Italy")));
		Assertions
			.assertTrue(
				countries.stream().anyMatch(cs -> cs.getClassid().equals("FR") && cs.getClassname().equals("France")));

		countries = tmp
			.filter(r -> r.getId().equals("50|355e65625b88::74009c567c81b4aa55c813db658734df"))
			.collect()
			.get(0)
			.getCountry();
		Assertions.assertEquals(2, countries.size());
		Assertions
			.assertTrue(
				countries.stream().anyMatch(cs -> cs.getClassid().equals("IT") && cs.getClassname().equals("Italy")));
		Assertions
			.assertTrue(
				countries
					.stream()
					.anyMatch(cs -> cs.getClassid().equals("NL") && cs.getClassname().equals("Netherlands")));

		countries = tmp
			.filter(r -> r.getId().equals("50|355e65625b88::54a1c76f520bb2c8da27d12e42891088"))
			.collect()
			.get(0)
			.getCountry();
		Assertions.assertEquals(2, countries.size());
		Assertions
			.assertTrue(
				countries.stream().anyMatch(cs -> cs.getClassid().equals("IT") && cs.getClassname().equals("Italy")));
		Assertions
			.assertTrue(
				countries.stream().anyMatch(cs -> cs.getClassid().equals("FR") && cs.getClassname().equals("France")));
	}

}
