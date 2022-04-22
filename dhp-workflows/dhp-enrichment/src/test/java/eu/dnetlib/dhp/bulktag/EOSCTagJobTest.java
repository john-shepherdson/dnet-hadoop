
package eu.dnetlib.dhp.bulktag;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static eu.dnetlib.dhp.bulktag.community.TaggingConstants.ZENODO_COMMUNITY_INDICATOR;

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

		spark.read().textFile(getClass().getResource("/eu/dnetlib/dhp/eosctag/jupyter/software").getPath())
				.map((MapFunction<String, Software>) value -> OBJECT_MAPPER.readValue(value, Software.class), Encoders.bean(Software.class))
				.write()
				.option("compression","gzip")
				.json(workingDir.toString() + "/input/software");

		spark.read().textFile(getClass().getResource("/eu/dnetlib/dhp/eosctag/jupyter/dataset").getPath())
				.map((MapFunction<String, Dataset>) value -> OBJECT_MAPPER.readValue(value, Dataset.class), Encoders.bean(Dataset.class))
				.write()
				.option("compression","gzip")
				.json(workingDir.toString() + "/input/dataset");

		spark.read().textFile(getClass().getResource("/eu/dnetlib/dhp/eosctag/jupyter/otherresearchproduct").getPath())
				.map((MapFunction<String, OtherResearchProduct>) value -> OBJECT_MAPPER.readValue(value, OtherResearchProduct.class), Encoders.bean(OtherResearchProduct.class))
				.write()
				.option("compression","gzip")
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

		Assertions.assertEquals(4, tmp.filter(s -> s.getSubject().stream().anyMatch(sbj -> sbj.getValue().equals("EOSC::Jupyter Notebook"))).count());

		Assertions.assertEquals(2, tmp.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4")).collect()
				.get(0).getSubject().size());
		Assertions.assertTrue(tmp.filter(sw -> sw.getId().equals("50|od______1582::4132f5ec9496f0d6adc7b00a50a56ff4")).collect()
				.get(0).getSubject().stream().anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));


		Assertions.assertEquals(5, tmp.filter(sw -> sw.getId().equals("50|od______1582::501b25d420f808c8eddcd9b16e917f11")).collect()
				.get(0).getSubject().size());
		Assertions.assertFalse(tmp.filter(sw -> sw.getId().equals("50|od______1582::501b25d420f808c8eddcd9b16e917f11")).collect()
				.get(0).getSubject().stream().anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));

		Assertions.assertEquals(9, tmp.filter(sw -> sw.getId().equals("50|od______1582::581621232a561b7e8b4952b18b8b0e56")).collect()
				.get(0).getSubject().size());
		Assertions.assertTrue(tmp.filter(sw -> sw.getId().equals("50|od______1582::581621232a561b7e8b4952b18b8b0e56")).collect()
				.get(0).getSubject().stream().anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));

		Assertions.assertEquals(5, tmp.filter(sw -> sw.getId().equals("50|od______1582::5aec1186054301b66c0c5dc35972a589")).collect()
				.get(0).getSubject().size());
		Assertions.assertFalse(tmp.filter(sw -> sw.getId().equals("50|od______1582::5aec1186054301b66c0c5dc35972a589")).collect()
				.get(0).getSubject().stream().anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));

		Assertions.assertEquals(9, tmp.filter(sw -> sw.getId().equals("50|od______1582::639909adfad9d708308f2aedb733e4a0")).collect()
				.get(0).getSubject().size());
		Assertions.assertTrue(tmp.filter(sw -> sw.getId().equals("50|od______1582::639909adfad9d708308f2aedb733e4a0")).collect()
				.get(0).getSubject().stream().anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));

		List<StructuredProperty> subjects = tmp.filter(sw -> sw.getId().equals("50|od______1582::6e7a9b21a2feef45673890432af34244")).collect()
				.get(0).getSubject();
		Assertions.assertEquals(8, subjects.size());
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("EOSC::Jupyter Notebook")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("jupyter")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("Modeling and Simulation")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("structure granulaire")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("algorithme")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("simulation numérique")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("flux de gaz")));
		Assertions.assertTrue(subjects.stream().anyMatch(s -> s.getValue().equals("flux de liquide")));


		Assertions.assertEquals(10, sc
				.textFile(workingDir.toString() + "/input/dataset")
				.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class)).count());

		Assertions.assertEquals(0, sc
				.textFile(workingDir.toString() + "/input/dataset")
				.map(item -> OBJECT_MAPPER.readValue(item, Dataset.class)).filter(ds -> ds.getSubject().stream().anyMatch(sbj -> sbj.getValue().equals("EOSC::Jupyter Notebook"))).count());


		Assertions.assertEquals(10, sc
				.textFile(workingDir.toString() + "/input/otherresearchproduct")
				.map(item -> OBJECT_MAPPER.readValue(item, OtherResearchProduct.class)).count());

		Assertions.assertEquals(0, sc
				.textFile(workingDir.toString() + "/input/otherresearchproduct")
				.map(item -> OBJECT_MAPPER.readValue(item, OtherResearchProduct.class)).filter(ds -> ds.getSubject().stream().anyMatch(sbj -> sbj.getValue().equals("EOSC::Jupyter Notebook"))).count());
	}


}
