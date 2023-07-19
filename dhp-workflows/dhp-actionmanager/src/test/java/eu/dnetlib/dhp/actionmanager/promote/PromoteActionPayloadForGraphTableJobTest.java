
package eu.dnetlib.dhp.actionmanager.promote;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;

public class PromoteActionPayloadForGraphTableJobTest {
	private static final ClassLoader cl = PromoteActionPayloadForGraphTableJobTest.class.getClassLoader();

	private static SparkSession spark;

	private Path workingDir;
	private Path inputDir;
	private Path inputGraphRootDir;
	private Path inputActionPayloadRootDir;
	private Path outputDir;

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	@BeforeAll
	public static void beforeAll() {
		SparkConf conf = new SparkConf();
		conf.setAppName(PromoteActionPayloadForGraphTableJobTest.class.getSimpleName());
		conf.setMaster("local");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());
		spark = SparkSession.builder().config(conf).getOrCreate();
	}

	@BeforeEach
	public void beforeEach() throws IOException {
		workingDir = Files.createTempDirectory(PromoteActionPayloadForGraphTableJobTest.class.getSimpleName());
		inputDir = workingDir.resolve("input");
		inputGraphRootDir = inputDir.resolve("graph");
		inputActionPayloadRootDir = inputDir.resolve("action_payload");
		outputDir = workingDir.resolve("output");
	}

	@AfterEach
	public void afterEach() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
	}

	@AfterAll
	public static void afterAll() {
		spark.stop();
	}

	@DisplayName("Job")
	@Nested
	class Main {

		@Test
		void shouldThrowWhenGraphTableClassIsNotASubClassOfActionPayloadClass() {
			// given
			Class<Relation> rowClazz = Relation.class;
			Class<OafEntity> actionPayloadClazz = OafEntity.class;

			// when
			RuntimeException exception = assertThrows(
				RuntimeException.class,
				() -> PromoteActionPayloadForGraphTableJob
					.main(
						new String[] {
							"-isSparkSessionManaged",
							Boolean.FALSE.toString(),
							"-inputGraphTablePath",
							"",
							"-graphTableClassName",
							rowClazz.getCanonicalName(),
							"-inputActionPayloadPath",
							"",
							"-actionPayloadClassName",
							actionPayloadClazz.getCanonicalName(),
							"-outputGraphTablePath",
							"",
							"-mergeAndGetStrategy",
							MergeAndGet.Strategy.SELECT_NEWER_AND_GET.name(),
							"--shouldGroupById",
							"true"
						}));

			// then
			String msg = String
				.format(
					"graph table class is not a subclass of action payload class: graph=%s, action=%s",
					rowClazz.getCanonicalName(), actionPayloadClazz.getCanonicalName());
			assertTrue(exception.getMessage().contains(msg));
		}

		@ParameterizedTest(name = "strategy: {0}, graph table: {1}, action payload: {2}")
		@MethodSource("eu.dnetlib.dhp.actionmanager.promote.PromoteActionPayloadForGraphTableJobTest#promoteJobTestParams")
		void shouldPromoteActionPayloadForGraphTable(
			MergeAndGet.Strategy strategy,
			Class<? extends Oaf> rowClazz,
			Class<? extends Oaf> actionPayloadClazz)
			throws Exception {
			// given
			Path inputGraphTableDir = createGraphTable(inputGraphRootDir, rowClazz);
			Path inputActionPayloadDir = createActionPayload(inputActionPayloadRootDir, rowClazz, actionPayloadClazz);
			Path outputGraphTableDir = outputDir.resolve("graph").resolve(rowClazz.getSimpleName().toLowerCase());

			// when
			PromoteActionPayloadForGraphTableJob
				.main(
					new String[] {
						"-isSparkSessionManaged",
						Boolean.FALSE.toString(),
						"-inputGraphTablePath",
						inputGraphTableDir.toString(),
						"-graphTableClassName",
						rowClazz.getCanonicalName(),
						"-inputActionPayloadPath",
						inputActionPayloadDir.toString(),
						"-actionPayloadClassName",
						actionPayloadClazz.getCanonicalName(),
						"-outputGraphTablePath",
						outputGraphTableDir.toString(),
						"-mergeAndGetStrategy",
						strategy.name(),
						"--shouldGroupById",
						"true"
					});

			// then
			assertTrue(Files.exists(outputGraphTableDir));

			List<? extends Oaf> actualOutputRows = readGraphTableFromJobOutput(outputGraphTableDir.toString(), rowClazz)
				.collectAsList()
				.stream()
				.sorted(Comparator.comparingInt(Object::hashCode))
				.collect(Collectors.toList());
			String expectedOutputGraphTableJsonDumpPath = resultFileLocation(strategy, rowClazz, actionPayloadClazz);
			Path expectedOutputGraphTableJsonDumpFile = Paths
				.get(
					Objects
						.requireNonNull(cl.getResource(expectedOutputGraphTableJsonDumpPath))
						.getFile());
			List<? extends Oaf> expectedOutputRows = readGraphTableFromJsonDump(
				expectedOutputGraphTableJsonDumpFile.toString(), rowClazz)
					.collectAsList()
					.stream()
					.sorted(Comparator.comparingInt(Object::hashCode))
					.collect(Collectors.toList());
			assertIterableEquals(expectedOutputRows, actualOutputRows);
		}
	}

	@Test
	void shouldPromoteActionPayload_custom() throws Exception {

		Class<? extends Oaf> rowClazz = Publication.class;
		Class<? extends Oaf> actionPayloadClazz = Result.class;
		MergeAndGet.Strategy strategy = MergeAndGet.Strategy.MERGE_FROM_AND_GET;

		// given
		Path inputGraphTableDir = createGraphTable(inputGraphRootDir, rowClazz);
		Path inputActionPayloadDir = createActionPayload(inputActionPayloadRootDir, rowClazz, actionPayloadClazz);
		Path outputGraphTableDir = outputDir.resolve("graph").resolve(rowClazz.getSimpleName().toLowerCase());

		// when
		PromoteActionPayloadForGraphTableJob
			.main(
				new String[] {
					"-isSparkSessionManaged",
					Boolean.FALSE.toString(),
					"-inputGraphTablePath",
					inputGraphTableDir.toString(),
					"-graphTableClassName",
					rowClazz.getCanonicalName(),
					"-inputActionPayloadPath",
					inputActionPayloadDir.toString(),
					"-actionPayloadClassName",
					actionPayloadClazz.getCanonicalName(),
					"-outputGraphTablePath",
					outputGraphTableDir.toString(),
					"-mergeAndGetStrategy",
					strategy.name(),
					"--shouldGroupById",
					"true"
				});

		// then
		assertTrue(Files.exists(outputGraphTableDir));

		List<? extends Oaf> actualOutputRows = readGraphTableFromJobOutput(outputGraphTableDir.toString(), rowClazz)
			.collectAsList()
			.stream()
			.sorted(Comparator.comparingInt(Object::hashCode))
			.collect(Collectors.toList());

		Publication p = actualOutputRows
			.stream()
			.map(o -> (Publication) o)
			.filter(o -> "50|4ScienceCRIS::6a67ed3daba1c380bf9de3c13ed9c879".equals(o.getId()))
			.findFirst()
			.get();

		assertNotNull(p.getMeasures());
		assertTrue(p.getMeasures().size() > 0);

	}

	public static Stream<Arguments> promoteJobTestParams() {
		return Stream
			.of(
				arguments(
					MergeAndGet.Strategy.MERGE_FROM_AND_GET,
					eu.dnetlib.dhp.schema.oaf.Dataset.class,
					eu.dnetlib.dhp.schema.oaf.Dataset.class),
				arguments(
					MergeAndGet.Strategy.MERGE_FROM_AND_GET,
					eu.dnetlib.dhp.schema.oaf.Dataset.class,
					eu.dnetlib.dhp.schema.oaf.Result.class),
				arguments(MergeAndGet.Strategy.MERGE_FROM_AND_GET, Datasource.class, Datasource.class),
				arguments(MergeAndGet.Strategy.MERGE_FROM_AND_GET, Organization.class, Organization.class),
				arguments(
					MergeAndGet.Strategy.MERGE_FROM_AND_GET,
					OtherResearchProduct.class,
					OtherResearchProduct.class),
				arguments(
					MergeAndGet.Strategy.MERGE_FROM_AND_GET, OtherResearchProduct.class, Result.class),
				arguments(MergeAndGet.Strategy.MERGE_FROM_AND_GET, Project.class, Project.class),
				arguments(MergeAndGet.Strategy.MERGE_FROM_AND_GET, Publication.class, Publication.class),
				arguments(MergeAndGet.Strategy.MERGE_FROM_AND_GET, Publication.class, Result.class),
				arguments(MergeAndGet.Strategy.MERGE_FROM_AND_GET, Relation.class, Relation.class),
				arguments(MergeAndGet.Strategy.MERGE_FROM_AND_GET, Software.class, Software.class),
				arguments(MergeAndGet.Strategy.MERGE_FROM_AND_GET, Software.class, Result.class));
	}

	private static <G extends Oaf> Path createGraphTable(Path inputGraphRootDir, Class<G> rowClazz) {
		String inputGraphTableJsonDumpPath = inputGraphTableJsonDumpLocation(rowClazz);
		Path inputGraphTableJsonDumpFile = Paths
			.get(Objects.requireNonNull(cl.getResource(inputGraphTableJsonDumpPath)).getFile());
		Dataset<G> rowDS = readGraphTableFromJsonDump(inputGraphTableJsonDumpFile.toString(), rowClazz);
		String inputGraphTableName = rowClazz.getSimpleName().toLowerCase();
		Path inputGraphTableDir = inputGraphRootDir.resolve(inputGraphTableName);
		writeGraphTableAaJobInput(rowDS, inputGraphTableDir.toString());
		return inputGraphTableDir;
	}

	private static String inputGraphTableJsonDumpLocation(Class<? extends Oaf> rowClazz) {
		return String
			.format(
				"%s/%s.json",
				"eu/dnetlib/dhp/actionmanager/promote/input/graph", rowClazz.getSimpleName().toLowerCase());
	}

	private static <G extends Oaf> Dataset<G> readGraphTableFromJsonDump(
		String path, Class<G> rowClazz) {
		return spark
			.read()
			.textFile(path)
			.map(
				(MapFunction<String, G>) json -> OBJECT_MAPPER.readValue(json, rowClazz),
				Encoders.bean(rowClazz));
	}

	private static <G extends Oaf> void writeGraphTableAaJobInput(Dataset<G> rowDS, String path) {
		rowDS.write().option("compression", "gzip").json(path);
	}

	private static <G extends Oaf, A extends Oaf> Path createActionPayload(
		Path inputActionPayloadRootDir, Class<G> rowClazz, Class<A> actionPayloadClazz) {
		String inputActionPayloadJsonDumpPath = inputActionPayloadJsonDumpLocation(rowClazz, actionPayloadClazz);
		Path inputActionPayloadJsonDumpFile = Paths
			.get(Objects.requireNonNull(cl.getResource(inputActionPayloadJsonDumpPath)).getFile());
		Dataset<String> actionPayloadDS = readActionPayloadFromJsonDump(inputActionPayloadJsonDumpFile.toString());
		Path inputActionPayloadDir = inputActionPayloadRootDir
			.resolve(actionPayloadClazz.getSimpleName().toLowerCase());
		writeActionPayloadAsJobInput(actionPayloadDS, inputActionPayloadDir.toString());
		return inputActionPayloadDir;
	}

	private static String inputActionPayloadJsonDumpLocation(
		Class<? extends Oaf> rowClazz, Class<? extends Oaf> actionPayloadClazz) {

		return String
			.format(
				"eu/dnetlib/dhp/actionmanager/promote/input/action_payload/%s_table/%s.json",
				rowClazz.getSimpleName().toLowerCase(), actionPayloadClazz.getSimpleName().toLowerCase());
	}

	private static Dataset<String> readActionPayloadFromJsonDump(String path) {
		return spark.read().textFile(path);
	}

	private static void writeActionPayloadAsJobInput(Dataset<String> actionPayloadDS, String path) {
		actionPayloadDS.withColumnRenamed("value", "payload").write().parquet(path);
	}

	private static <G extends Oaf> Dataset<G> readGraphTableFromJobOutput(
		String path, Class<G> rowClazz) {
		return spark
			.read()
			.textFile(path)
			.map(
				(MapFunction<String, G>) json -> OBJECT_MAPPER.readValue(json, rowClazz),
				Encoders.bean(rowClazz));
	}

	private static String resultFileLocation(
		MergeAndGet.Strategy strategy,
		Class<? extends Oaf> rowClazz,
		Class<? extends Oaf> actionPayloadClazz) {
		return String
			.format(
				"eu/dnetlib/dhp/actionmanager/promote/output/graph/%s/%s/%s_action_payload/result.json",
				strategy.name().toLowerCase(),
				rowClazz.getSimpleName().toLowerCase(),
				actionPayloadClazz.getSimpleName().toLowerCase());
	}
}
