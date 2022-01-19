
package eu.dnetlib.dhp.oa.graph.group;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.merge.GroupEntitiesSparkJob;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Result;

public class GroupEntitiesSparkJobTest {

	private static SparkSession spark;

	private Path workingDir;
	private Path graphInputPath;

	private Path outputPath;

	@BeforeAll
	public static void beforeAll() {
		SparkConf conf = new SparkConf();
		conf.setAppName(GroupEntitiesSparkJob.class.getSimpleName());
		conf.setMaster("local");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());
		spark = SparkSession.builder().config(conf).getOrCreate();
	}

	@BeforeEach
	public void beforeEach() throws IOException, URISyntaxException {
		workingDir = Files.createTempDirectory(GroupEntitiesSparkJob.class.getSimpleName());
		graphInputPath = Paths.get(ClassLoader.getSystemResource("eu/dnetlib/dhp/oa/graph/group").toURI());
		outputPath = workingDir.resolve("output");
	}

	@AfterEach
	public void afterEach() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
	}

	@AfterAll
	public static void afterAll() {
		spark.stop();
	}

	@Test
	void testGroupEntities() throws Exception {
		GroupEntitiesSparkJob.main(new String[] {
			"-isSparkSessionManaged",
			Boolean.FALSE.toString(),
			"-graphInputPath",
			graphInputPath.toString(),
			"-outputPath",
			outputPath.toString()
		});

		ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		Dataset<Result> output = spark
			.read()
			.textFile(outputPath.toString())
			.map((MapFunction<String, String>) s -> StringUtils.substringAfter(s, "|"), Encoders.STRING())
			.map((MapFunction<String, Result>) s -> mapper.readValue(s, Result.class), Encoders.bean(Result.class));

		Assertions
			.assertEquals(
				1,
				output
					.filter(
						(FilterFunction<Result>) r -> "50|doi_________::09821844208a5cd6300b2bfb13bca1b9"
							.equals(r.getId()) &&
							r.getCollectedfrom().stream().anyMatch(kv -> kv.getValue().equalsIgnoreCase("zenodo")))
					.count());
	}

}
