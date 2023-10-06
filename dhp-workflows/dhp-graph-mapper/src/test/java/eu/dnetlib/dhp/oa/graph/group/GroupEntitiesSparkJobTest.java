
package eu.dnetlib.dhp.oa.graph.group;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.merge.GroupEntitiesSparkJob;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.utils.DHPUtils;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GroupEntitiesSparkJobTest {

	private static SparkSession spark;

	private static ObjectMapper mapper = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	private static Path workingDir;
	private Path dataInputPath;

	private Path checkpointPath;

	private Path outputPath;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(GroupEntitiesSparkJob.class.getSimpleName());

		SparkConf conf = new SparkConf();
		conf.setAppName(GroupEntitiesSparkJob.class.getSimpleName());
		conf.setMaster("local");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());
		spark = SparkSession.builder().config(conf).getOrCreate();
	}

	@BeforeEach
	public void beforeEach() throws IOException, URISyntaxException {
		dataInputPath = Paths.get(ClassLoader.getSystemResource("eu/dnetlib/dhp/oa/graph/group").toURI());
		checkpointPath = workingDir.resolve("grouped_entity");
		outputPath = workingDir.resolve("dispatched_entity");
	}

	@AfterAll
	public static void afterAll() throws IOException {
		spark.stop();
		FileUtils.deleteDirectory(workingDir.toFile());
	}

	@Test
	@Order(1)
	void testGroupEntities() throws Exception {
		GroupEntitiesSparkJob.main(new String[] {
			"-isSparkSessionManaged",
			Boolean.FALSE.toString(),
			"-graphInputPath",
			dataInputPath.toString(),
			"-checkpointPath",
			checkpointPath.toString(),
			"-outputPath",
			outputPath.toString(),
			"-filterInvisible",
			Boolean.FALSE.toString()
		});

		Dataset<OafEntity> checkpointTable = spark
			.read()
			.load(checkpointPath.toString())
			.selectExpr("COALESCE(*)")
			.as(Encoders.kryo(OafEntity.class));

		assertEquals(
			1,
			checkpointTable
				.filter(
					(FilterFunction<OafEntity>) r -> "50|doi_________::09821844208a5cd6300b2bfb13bca1b9"
						.equals(r.getId()) &&
						r.getCollectedfrom().stream().anyMatch(kv -> kv.getValue().equalsIgnoreCase("zenodo")))
				.count());

		Dataset<Result> output = spark
			.read()
			.textFile(
				DHPUtils
					.toSeq(
						HdfsSupport
							.listFiles(outputPath.toString(), spark.sparkContext().hadoopConfiguration())))
			.map((MapFunction<String, Result>) s -> mapper.readValue(s, Result.class), Encoders.bean(Result.class));

		assertEquals(3, output.count());
		assertEquals(
			2,
			output
				.map((MapFunction<Result, String>) r -> r.getResulttype().getClassid(), Encoders.STRING())
				.filter((FilterFunction<String>) s -> s.equals("publication"))
				.count());
		assertEquals(
			1,
			output
				.map((MapFunction<Result, String>) r -> r.getResulttype().getClassid(), Encoders.STRING())
				.filter((FilterFunction<String>) s -> s.equals("dataset"))
				.count());
	}
}
