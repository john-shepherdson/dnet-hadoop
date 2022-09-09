
package eu.dnetlib.dhp.bulktag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
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

import eu.dnetlib.dhp.bulktag.eosc.SparkEoscTag;
import eu.dnetlib.dhp.schema.oaf.*;

public class EOSCTagJobTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static final Logger log = LoggerFactory.getLogger(EOSCTagJobTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(EOSCTagJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();
		conf.setAppName(EOSCTagJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		spark = SparkSession
			.builder()
			.appName(EOSCTagJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void jupyterUpdatesTest() throws Exception {

		spark
			.read()
			.textFile(getClass().getResource("/eu/dnetlib/dhp/eosctag/jupyter/software").getPath())
			.map(
				(MapFunction<String, Software>) value -> OBJECT_MAPPER.readValue(value, Software.class),
				Encoders.bean(Software.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/input/software");

		spark
			.read()
			.textFile(getClass().getResource("/eu/dnetlib/dhp/eosctag/jupyter/dataset").getPath())
			.map(
				(MapFunction<String, Dataset>) value -> OBJECT_MAPPER.readValue(value, Dataset.class),
				Encoders.bean(Dataset.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/input/dataset");

		spark
			.read()
			.textFile(getClass().getResource("/eu/dnetlib/dhp/eosctag/jupyter/otherresearchproduct").getPath())
			.map(
				(MapFunction<String, OtherResearchProduct>) value -> OBJECT_MAPPER
					.readValue(value, OtherResearchProduct.class),
				Encoders.bean(OtherResearchProduct.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/input/otherresearchproduct");

		SparkEoscTag
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath",
					workingDir.toString() + "/input",
					"-workingPath", workingDir.toString() + "/working"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Software> tmp = sc
			.textFile(workingDir.toString() + "/input/software")
			.map(item -> OBJECT_MAPPER.readValue(item, Software.class));

		Assertions.assertEquals(10, tmp.count());

		Assertions
			.assertEquals(
				4,
				tmp
					.filter(s -> s.getEoscifguidelines() != null)
					.filter(
						s -> s
							.getEoscifguidelines()
							.stream()
							.anyMatch(eig -> eig.getCode().equals("EOSC::Jupyter Notebook")))
					.count());

		Assertions
			.assertEquals(
				1, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.size());

		Assertions
			.assertEquals(
				1, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertTrue(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.stream()
					.anyMatch(s -> s.getCode().equals("EOSC::Jupyter Notebook")));

		Assertions
			.assertFalse(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));

		Assertions
			.assertEquals(
				5, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::501b25d420f808c8eddcd9b16e917f11"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::501b25d420f808c8eddcd9b16e917f11"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));

		Assertions
			.assertTrue(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::501b25d420f808c8eddcd9b16e917f11"))
					.collect()
					.get(0)
					.getEoscifguidelines() == null);

		Assertions
			.assertEquals(
				8, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::581621232a561b7e8b4952b18b8b0e56"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::581621232a561b7e8b4952b18b8b0e56"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));
		Assertions
			.assertEquals(
				1, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::581621232a561b7e8b4952b18b8b0e56"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.size());
		Assertions
			.assertTrue(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::581621232a561b7e8b4952b18b8b0e56"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.stream()
					.anyMatch(s -> s.getCode().equals("EOSC::Jupyter Notebook")));

		Assertions
			.assertEquals(
				5, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::5aec1186054301b66c0c5dc35972a589"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::5aec1186054301b66c0c5dc35972a589"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));
		Assertions
			.assertTrue(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::5aec1186054301b66c0c5dc35972a589"))
					.collect()
					.get(0)
					.getEoscifguidelines() == null);

		Assertions
			.assertEquals(
				8, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::639909adfad9d708308f2aedb733e4a0"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::639909adfad9d708308f2aedb733e4a0"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));
		Assertions
			.assertEquals(
				1,
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::639909adfad9d708308f2aedb733e4a0"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.size());
		Assertions
			.assertTrue(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::639909adfad9d708308f2aedb733e4a0"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.stream()
					.anyMatch(s -> s.getCode().equals("EOSC::Jupyter Notebook")));

		List<Subject> subjects = tmp
			.filter(sw -> sw.getId().equals("50|od______1582::6e7a9b21a2feef45673890432af34244"))
			.collect()
			.get(0)
			.getSubject();
		Assertions.assertEquals(7, subjects.size());
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("jupyter")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("Modeling and Simulation")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("structure granulaire")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("algorithme")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("simulation numérique")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("flux de gaz")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("flux de liquide")));

		Assertions
			.assertEquals(
				10, sc
					.textFile(workingDir.toString() + "/input/dataset")
					.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class))
					.count());

		Assertions
			.assertEquals(
				0, sc
					.textFile(workingDir.toString() + "/input/dataset")
					.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class))
					.filter(
						ds -> ds.getSubject().stream().anyMatch(sbj -> sbj.getValue().equals("EOSC::Jupyter Notebook")))
					.count());
		Assertions
			.assertEquals(
				0, sc
					.textFile(workingDir.toString() + "/input/dataset")
					.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class))
					.filter(
						ds -> ds
							.getEoscifguidelines()
							.stream()
							.anyMatch(eig -> eig.getCode().equals("EOSC::Jupyter Notebook")))
					.count());

		Assertions
			.assertEquals(
				10, sc
					.textFile(workingDir.toString() + "/input/otherresearchproduct")
					.map(item -> OBJECT_MAPPER.readValue(item, OtherResearchProduct.class))
					.count());

		Assertions
			.assertEquals(
				0, sc
					.textFile(workingDir.toString() + "/input/otherresearchproduct")
					.map(item -> OBJECT_MAPPER.readValue(item, OtherResearchProduct.class))
					.filter(
						orp -> orp
							.getSubject()
							.stream()
							.anyMatch(sbj -> sbj.getValue().equals("EOSC::Jupyter Notebook")))
					.count());

		Assertions
			.assertEquals(
				0, sc
					.textFile(workingDir.toString() + "/input/otherresearchproduct")
					.map(item -> OBJECT_MAPPER.readValue(item, OtherResearchProduct.class))
					.filter(
						orp -> orp
							.getSubject()
							.stream()
							.anyMatch(eig -> eig.getValue().equals("EOSC::Jupyter Notebook")))
					.count());

		// spark.stop();
	}

	@Test
	void galaxyUpdatesTest() throws Exception {
		spark
			.read()
			.textFile(getClass().getResource("/eu/dnetlib/dhp/eosctag/galaxy/software").getPath())
			.map(
				(MapFunction<String, Software>) value -> OBJECT_MAPPER.readValue(value, Software.class),
				Encoders.bean(Software.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/input/software");

		spark
			.read()
			.textFile(getClass().getResource("/eu/dnetlib/dhp/eosctag/galaxy/dataset").getPath())
			.map(
				(MapFunction<String, Dataset>) value -> OBJECT_MAPPER.readValue(value, Dataset.class),
				Encoders.bean(Dataset.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/input/dataset");

		spark
			.read()
			.textFile(getClass().getResource("/eu/dnetlib/dhp/eosctag/galaxy/otherresearchproduct").getPath())
			.map(
				(MapFunction<String, OtherResearchProduct>) value -> OBJECT_MAPPER
					.readValue(value, OtherResearchProduct.class),
				Encoders.bean(OtherResearchProduct.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/input/otherresearchproduct");

		SparkEoscTag
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath",
					workingDir.toString() + "/input",
					"-workingPath", workingDir.toString() + "/working"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Software> tmp = sc
			.textFile(workingDir.toString() + "/input/software")
			.map(item -> OBJECT_MAPPER.readValue(item, Software.class));

		Assertions.assertEquals(11, tmp.count());

		Assertions
			.assertEquals(
				0,
				tmp
					.filter(
						s -> s.getSubject().stream().anyMatch(sbj -> sbj.getValue().equals("EOSC::Galaxy Workflow")))
					.count());
		Assertions
			.assertEquals(
				1,
				tmp
					.filter(
						s -> s.getEoscifguidelines() != null)
					.count());
		Assertions
			.assertEquals(
				1,
				tmp
					.filter(
						s -> s.getEoscifguidelines() != null)
					.filter(
						s -> s
							.getEoscifguidelines()
							.stream()
							.anyMatch(eig -> eig.getCode().equals("EOSC::Galaxy Workflow")))
					.count());

		Assertions
			.assertEquals(
				1, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Galaxy Workflow")));

		Assertions
			.assertEquals(
				1, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.size());
		Assertions
			.assertTrue(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.stream()
					.anyMatch(eig -> eig.getCode().equals("EOSC::Galaxy Workflow")));

		Assertions
			.assertEquals(
				5, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::501b25d420f808c8eddcd9b16e917f11"))
					.collect()
					.get(0)
					.getSubject()
					.size());

		Assertions
			.assertEquals(
				8, tmp
					.filter(sw -> sw.getId().equals("50|od______1582::581621232a561b7e8b4952b18b8b0e56"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				tmp
					.filter(sw -> sw.getId().equals("50|od______1582::581621232a561b7e8b4952b18b8b0e56"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Galaxy Workflow")));

		JavaRDD<OtherResearchProduct> orp = sc
			.textFile(workingDir.toString() + "/input/otherresearchproduct")
			.map(item -> OBJECT_MAPPER.readValue(item, OtherResearchProduct.class));

		Assertions.assertEquals(10, orp.count());

		Assertions
			.assertEquals(
				0,
				orp
					.filter(
						s -> s.getSubject().stream().anyMatch(sbj -> sbj.getValue().equals("EOSC::Galaxy Workflow")))
					.count());
		orp.foreach(o -> System.out.println(OBJECT_MAPPER.writeValueAsString(o)));

		Assertions
			.assertEquals(
				1, orp
					.filter(o -> o.getEoscifguidelines() != null)
					.filter(
						o -> o
							.getEoscifguidelines()
							.stream()
							.anyMatch(eig -> eig.getCode().equals("EOSC::Galaxy Workflow")))
					.count());

		Assertions
			.assertEquals(
				2, orp
					.filter(sw -> sw.getId().equals("50|od______2017::0750a4d0782265873d669520f5e33c07"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				orp
					.filter(sw -> sw.getId().equals("50|od______2017::0750a4d0782265873d669520f5e33c07"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Galaxy Workflow")));
		Assertions
			.assertEquals(
				1, orp
					.filter(sw -> sw.getId().equals("50|od______2017::0750a4d0782265873d669520f5e33c07"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.size());
		Assertions
			.assertTrue(
				orp
					.filter(sw -> sw.getId().equals("50|od______2017::0750a4d0782265873d669520f5e33c07"))
					.collect()
					.get(0)
					.getEoscifguidelines()
					.stream()
					.anyMatch(s -> s.getCode().equals("EOSC::Galaxy Workflow")));

		Assertions
			.assertEquals(
				2, orp
					.filter(sw -> sw.getId().equals("50|od______2017::1bd97baef19dbd2db3203b112bb83bc5"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				orp
					.filter(sw -> sw.getId().equals("50|od______2017::1bd97baef19dbd2db3203b112bb83bc5"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Galaxy Workflow")));

		Assertions
			.assertEquals(
				2, orp
					.filter(sw -> sw.getId().equals("50|od______2017::1e400f1747487fd15998735c41a55c72"))
					.collect()
					.get(0)
					.getSubject()
					.size());
		Assertions
			.assertFalse(
				orp
					.filter(sw -> sw.getId().equals("50|od______2017::1e400f1747487fd15998735c41a55c72"))
					.collect()
					.get(0)
					.getSubject()
					.stream()
					.anyMatch(s -> s.getValue().equals("EOSC::Galaxy Workflow")));

	}

	@Test
	void twitterUpdatesTest() throws Exception {
		spark
			.read()
			.textFile(getClass().getResource("/eu/dnetlib/dhp/eosctag/twitter/software").getPath())
			.map(
				(MapFunction<String, Software>) value -> OBJECT_MAPPER.readValue(value, Software.class),
				Encoders.bean(Software.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/input/software");

		spark
			.read()
			.textFile(getClass().getResource("/eu/dnetlib/dhp/eosctag/twitter/dataset").getPath())
			.map(
				(MapFunction<String, Dataset>) value -> OBJECT_MAPPER.readValue(value, Dataset.class),
				Encoders.bean(Dataset.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/input/dataset");

		spark
			.read()
			.textFile(getClass().getResource("/eu/dnetlib/dhp/eosctag/twitter/otherresearchproduct").getPath())
			.map(
				(MapFunction<String, OtherResearchProduct>) value -> OBJECT_MAPPER
					.readValue(value, OtherResearchProduct.class),
				Encoders.bean(OtherResearchProduct.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir.toString() + "/input/otherresearchproduct");

		SparkEoscTag
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-sourcePath",
					workingDir.toString() + "/input",
					"-workingPath", workingDir.toString() + "/working"

				});

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Software> tmp = sc
			.textFile(workingDir.toString() + "/input/software")
			.map(item -> OBJECT_MAPPER.readValue(item, Software.class));

		Assertions.assertEquals(10, tmp.count());

		Assertions
			.assertEquals(
				0,
				tmp
					.filter(s -> s.getSubject().stream().anyMatch(sbj -> sbj.getValue().equals("EOSC::Twitter Data")))
					.count());

		JavaRDD<OtherResearchProduct> orp = sc
			.textFile(workingDir.toString() + "/input/otherresearchproduct")
			.map(item -> OBJECT_MAPPER.readValue(item, OtherResearchProduct.class));

		Assertions.assertEquals(10, orp.count());

		Assertions
			.assertEquals(
				0,
				orp
					.filter(s -> s.getSubject().stream().anyMatch(sbj -> sbj.getValue().equals("EOSC::Twitter Data")))
					.count());
		Assertions
			.assertEquals(
				3,
				orp
					.filter(
						s -> s
							.getEoscifguidelines()
							.stream()
							.anyMatch(eig -> eig.getCode().equals("EOSC::Twitter Data")))
					.count());

		JavaRDD<Dataset> dats = sc
			.textFile(workingDir.toString() + "/input/dataset")
			.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class));

		Assertions.assertEquals(11, dats.count());

		Assertions
			.assertEquals(
				3,
				dats
					.filter(
						s -> s
							.getEoscifguidelines()
							.stream()
							.anyMatch(eig -> eig.getCode().equals("EOSC::Twitter Data")))
					.count());

	}
}
