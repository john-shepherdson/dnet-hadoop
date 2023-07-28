
package eu.dnetlib.dhp.actionmanager.partition;

import static eu.dnetlib.dhp.common.ThrowingSupport.rethrowAsRuntimeException;
import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static scala.collection.JavaConversions.mutableSeqAsJavaList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.ISClient;
import eu.dnetlib.dhp.actionmanager.promote.PromoteActionPayloadForGraphTableJobTest;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;
import scala.collection.mutable.Seq;

@ExtendWith(MockitoExtension.class)
public class PartitionActionSetsByPayloadTypeJobTest {
	private static final ClassLoader cl = PartitionActionSetsByPayloadTypeJobTest.class.getClassLoader();

	private static Configuration configuration;
	private static SparkSession spark;

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	private static final StructType ATOMIC_ACTION_SCHEMA = StructType$.MODULE$
		.apply(
			Arrays
				.asList(
					StructField$.MODULE$.apply("clazz", DataTypes.StringType, false, Metadata.empty()),
					StructField$.MODULE$
						.apply(
							"payload", DataTypes.StringType, false, Metadata.empty())));

	@BeforeAll
	public static void beforeAll() throws IOException {
		configuration = Job.getInstance().getConfiguration();
		SparkConf conf = new SparkConf();
		conf.setAppName(PromoteActionPayloadForGraphTableJobTest.class.getSimpleName());
		conf.setMaster("local");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		spark = SparkSession.builder().config(conf).getOrCreate();
	}

	@AfterAll
	public static void afterAll() {
		spark.stop();
	}

	@DisplayName("Job")
	@Nested
	class Main {

		@Mock
		private ISClient isClient;

		@Test
		void shouldPartitionActionSetsByPayloadType(@TempDir Path workingDir) throws Exception {
			// given
			Path inputActionSetsBaseDir = workingDir.resolve("input").resolve("action_sets");
			Path outputDir = workingDir.resolve("output");

			Map<String, List<String>> oafsByClassName = createActionSets(inputActionSetsBaseDir);

			List<String> inputActionSetsPaths = resolveInputActionSetPaths(inputActionSetsBaseDir);

			// when
			Mockito
				.when(isClient.getLatestRawsetPaths(Mockito.anyString()))
				.thenReturn(inputActionSetsPaths);

			PartitionActionSetsByPayloadTypeJob job = new PartitionActionSetsByPayloadTypeJob();
			job.setIsClient(isClient);
			job
				.run(
					Boolean.FALSE,
					"", // it can be empty we're mocking the response from isClient
					// to
					// resolve the
					// paths
					outputDir.toString());

			// then
			Files.exists(outputDir);

			assertForOafType(outputDir, oafsByClassName, eu.dnetlib.dhp.schema.oaf.Dataset.class);
			assertForOafType(outputDir, oafsByClassName, Datasource.class);
			assertForOafType(outputDir, oafsByClassName, Organization.class);
			assertForOafType(outputDir, oafsByClassName, OtherResearchProduct.class);
			assertForOafType(outputDir, oafsByClassName, Project.class);
			assertForOafType(outputDir, oafsByClassName, Publication.class);
			assertForOafType(outputDir, oafsByClassName, Result.class);
			assertForOafType(outputDir, oafsByClassName, Relation.class);
			assertForOafType(outputDir, oafsByClassName, Software.class);
		}
	}

	private List<String> resolveInputActionSetPaths(Path inputActionSetsBaseDir) throws IOException {
		Path inputActionSetJsonDumpsDir = getInputActionSetJsonDumpsDir();
		return Files
			.list(inputActionSetJsonDumpsDir)
			.map(
				path -> {
					String inputActionSetId = path.getFileName().toString();
					return inputActionSetsBaseDir.resolve(inputActionSetId).toString();
				})
			.collect(Collectors.toCollection(ArrayList::new));
	}

	private static Map<String, List<String>> createActionSets(Path inputActionSetsDir)
		throws IOException {
		Path inputActionSetJsonDumpsDir = getInputActionSetJsonDumpsDir();

		Map<String, List<String>> oafsByType = new HashMap<>();
		Files
			.list(inputActionSetJsonDumpsDir)
			.forEach(
				inputActionSetJsonDumpFile -> {
					String inputActionSetId = inputActionSetJsonDumpFile.getFileName().toString();
					Path inputActionSetDir = inputActionSetsDir.resolve(inputActionSetId);

					Dataset<String> actionDS = readActionsFromJsonDump(inputActionSetJsonDumpFile.toString()).cache();

					writeActionsAsJobInput(actionDS, inputActionSetId, inputActionSetDir.toString());

					Map<String, List<String>> actionSetOafsByType = actionDS
						.withColumn("atomic_action", from_json(col("value"), ATOMIC_ACTION_SCHEMA))
						.select(expr("atomic_action.*"))
						.groupBy(col("clazz"))
						.agg(collect_list(col("payload")).as("payload_list"))
						.collectAsList()
						.stream()
						.map(
							row -> new AbstractMap.SimpleEntry<>(
								row.<String> getAs("clazz"),
								mutableSeqAsJavaList(row.<Seq<String>> getAs("payload_list"))))
						.collect(
							Collectors
								.toMap(
									AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

					actionSetOafsByType
						.keySet()
						.forEach(
							x -> {
								if (oafsByType.containsKey(x)) {
									List<String> collected = new ArrayList<>();
									collected.addAll(oafsByType.get(x));
									collected.addAll(actionSetOafsByType.get(x));
									oafsByType.put(x, collected);
								} else {
									oafsByType.put(x, actionSetOafsByType.get(x));
								}
							});
				});

		return oafsByType;
	}

	private static Path getInputActionSetJsonDumpsDir() {
		return Paths
			.get(
				Objects
					.requireNonNull(cl.getResource("eu/dnetlib/dhp/actionmanager/partition/input/"))
					.getFile());
	}

	private static Dataset<String> readActionsFromJsonDump(String path) {
		return spark.read().textFile(path);
	}

	private static void writeActionsAsJobInput(
		Dataset<String> actionDS, String inputActionSetId, String path) {
		actionDS
			.javaRDD()
			.mapToPair(json -> new Tuple2<>(new Text(inputActionSetId), new Text(json)))
			.saveAsNewAPIHadoopFile(
				path, Text.class, Text.class, SequenceFileOutputFormat.class, configuration);
	}

	private static <T extends Oaf> void assertForOafType(
		Path outputDir, Map<String, List<String>> oafsByClassName, Class<T> clazz) {
		Path outputDatasetDir = outputDir.resolve(String.format("clazz=%s", clazz.getCanonicalName()));
		Files.exists(outputDatasetDir);

		List<T> actuals = readActionPayloadFromJobOutput(outputDatasetDir.toString(), clazz).collectAsList();
		actuals.sort(Comparator.comparingInt(Object::hashCode));

		List<T> expecteds = oafsByClassName
			.get(clazz.getCanonicalName())
			.stream()
			.map(json -> mapToOaf(json, clazz))
			.sorted(Comparator.comparingInt(Object::hashCode))
			.collect(Collectors.toList());

		assertIterableEquals(expecteds, actuals);
	}

	private static <T extends Oaf> Dataset<T> readActionPayloadFromJobOutput(
		String path, Class<T> clazz) {
		return spark
			.read()
			.parquet(path)
			.map(
				(MapFunction<Row, T>) value -> OBJECT_MAPPER.readValue(value.<String> getAs("payload"), clazz),
				Encoders.bean(clazz));
	}

	private static <T extends Oaf> T mapToOaf(String json, Class<T> clazz) {
		return rethrowAsRuntimeException(
			() -> OBJECT_MAPPER.readValue(json, clazz),
			String
				.format(
					"failed to map json to class: json=%s, class=%s", json, clazz.getCanonicalName()));
	}
}
