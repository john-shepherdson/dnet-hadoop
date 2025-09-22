
package eu.dnetlib.dhp.oa.graph.group;

import static eu.dnetlib.dhp.schema.common.ModelConstants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.oa.merge.GroupEntitiesSparkJob;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.InstanceTypeMapping;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GroupEntitiesSparkJobTest {

	@Mock
	private ISLookUpService isLookUpService;

	private VocabularyGroup vocabularies;

	private static SparkSession spark;

	private static ObjectMapper mapper = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	private static Path workingDir;
	private Path dataInputPath;

	private Path checkpointPath;

	private Path outputPath;

	@BeforeAll
	public static void beforeAll() throws IOException {
		workingDir = Files.createTempDirectory(GroupEntitiesSparkJobTest.class.getSimpleName());

		SparkConf conf = new SparkConf();
		conf.setAppName(GroupEntitiesSparkJobTest.class.getSimpleName());
		conf.setMaster("local");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());
		spark = SparkSession.builder().config(conf).getOrCreate();
	}

	@BeforeEach
	public void beforeEach() throws IOException, URISyntaxException, ISLookUpException {
		dataInputPath = Paths.get(ClassLoader.getSystemResource("eu/dnetlib/dhp/oa/graph/group").toURI());
		checkpointPath = workingDir.resolve("grouped_entity");
		outputPath = workingDir.resolve("dispatched_entity");

		lenient().when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARIES_XQUERY)).thenReturn(vocs());
		lenient()
			.when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARY_SYNONYMS_XQUERY))
			.thenReturn(synonyms());

		vocabularies = VocabularyGroup.loadVocsFromIS(isLookUpService);
	}

	@AfterAll
	public static void afterAll() throws IOException {
		spark.stop();
		FileUtils.deleteDirectory(workingDir.toFile());
	}

	@Test
	@Order(1)
	void testGroupEntities() throws Exception {
		new GroupEntitiesSparkJob(
			args(
				"/eu/dnetlib/dhp/oa/merge/group_graph_entities_parameters.json",
				new String[] {
					"--isSparkSessionManaged", Boolean.FALSE.toString(),
					"--graphInputPath", dataInputPath.toString(),
					"--checkpointPath", checkpointPath.toString(),
					"--outputPath", outputPath.toString(),
					"--filterInvisible", Boolean.FALSE.toString(),
					"--isLookupUrl", "lookupurl"
				})).run(false, isLookUpService);

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

		List<Result> output = spark
			.read()
			.textFile(
				DHPUtils
					.toSeq(
						HdfsSupport
							.listFiles(outputPath.toString(), spark.sparkContext().hadoopConfiguration()))
					.toSeq())
			.map((MapFunction<String, Result>) s -> mapper.readValue(s, Result.class), Encoders.bean(Result.class))
            .collectAsList();

		assertEquals(3, output.size());

		Set<String> resultTypes = output
                .stream()
			.map(value -> value.getResulttype().getClassid())
			.collect(Collectors.toSet());

		assertEquals(1, resultTypes.size());

		assertEquals(
			3,
			output
                .stream()
				.map(r -> r.getResulttype().getClassid())
				.filter(s -> s.equals("publication"))
				.count());
		assertEquals(
			0,
			output
                .stream()
				.map(r -> r.getResulttype().getClassid())
				.filter(s -> s.equals("dataset"))
				.count());

		Result result = output
            .stream()
			.filter(r -> "50|doi_________::09821844208a5cd6300b2bfb13bca1b9".equals(r.getId()))
			.findFirst()
            .get();

		result.getInstance().forEach(instance -> {
			Optional<InstanceTypeMapping> coarType = instance
				.getInstanceTypeMapping()
				.stream()
				.filter(itm -> OPENAIRE_COAR_RESOURCE_TYPES_3_1.equals(itm.getVocabularyName()))
				.filter(itm -> "journal-article".equals(itm.getOriginalType()))
				.findFirst();

			assertTrue(coarType.isPresent());
			assertEquals("http://purl.org/coar/resource_type/c_2df8fbb1", coarType.get().getTypeCode());
			assertEquals("research article", coarType.get().getTypeLabel());
		});

		final List<Result> filtered = output
                .stream()
                .filter(r -> "50|DansKnawCris::203a27996ddc0fd1948258e5b7dec61c".equals(r.getId()))
                .collect(Collectors.toList());
		assertEquals(1, filtered.size());
		result = filtered.get(0);

		result
			.getInstance()
			.stream()
			.flatMap(instance -> instance.getInstanceTypeMapping().stream())
			.filter(itm -> OPENAIRE_COAR_RESOURCE_TYPES_3_1.equals(itm.getVocabularyName()))
			.filter(itm -> "Patent".equals(itm.getOriginalType()))
			.forEach(itm -> {
				assertEquals("http://purl.org/coar/resource_type/c_15cd", itm.getTypeCode());
				assertEquals("patent", itm.getTypeLabel());
			});
	}

	private List<String> vocs() throws IOException {
		return IOUtils
			.readLines(
				Objects
					.requireNonNull(
						getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/terms.txt")),
				Charset.defaultCharset());
	}

	private List<String> synonyms() throws IOException {
		return IOUtils
			.readLines(
				Objects
					.requireNonNull(
						getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/synonyms.txt")),
				Charset.defaultCharset());
	}

	private ArgumentApplicationParser args(String paramSpecs, String[] args) throws IOException, ParseException {
		ArgumentApplicationParser parser = new ArgumentApplicationParser(classPathResourceAsString(paramSpecs));
		parser.parseArgument(args);
		return parser;
	}

	private static String classPathResourceAsString(String path) throws IOException {
		return IOUtils
			.toString(
				Objects
					.requireNonNull(
						GroupEntitiesSparkJobTest.class.getResourceAsStream(path)),
				Charset.defaultCharset());
	}

}
