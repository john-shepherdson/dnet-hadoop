
package eu.dnetlib.dhp.collection;

import static eu.dnetlib.dhp.common.Constants.MDSTORE_DATA_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.aggregation.AbstractVocabularyTest;
import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion;
import eu.dnetlib.dhp.schema.mdstore.MetadataRecord;
import eu.dnetlib.dhp.schema.mdstore.Provenance;
import eu.dnetlib.dhp.transformation.TransformSparkJobNode;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(MockitoExtension.class)
public class GenerateNativeStoreSparkJobTest extends AbstractVocabularyTest {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static SparkSession spark;

	private static Path workingDir;

	private static Encoder<MetadataRecord> encoder;

	private static final String encoding = "XML";
	private static final String dateOfCollection = System.currentTimeMillis() + "";
	private static final String xpath = "//*[local-name()='header']/*[local-name()='identifier']";
	private static String provenance;

	private static final Logger log = LoggerFactory.getLogger(GenerateNativeStoreSparkJobTest.class);

	@BeforeAll
	public static void beforeAll() throws IOException {
		provenance = IOUtils
			.toString(
				GenerateNativeStoreSparkJobTest.class
					.getResourceAsStream("/eu/dnetlib/dhp/collection/provenance.json"));
		workingDir = Files.createTempDirectory(GenerateNativeStoreSparkJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();

		conf.setAppName(GenerateNativeStoreSparkJobTest.class.getSimpleName());

		conf.setMaster("local[*]");
		conf.set("spark.driver.host", "localhost");
		conf.set("hive.metastore.local", "true");
		conf.set("spark.ui.enabled", "false");
		conf.set("spark.sql.warehouse.dir", workingDir.toString());
		conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

		encoder = Encoders.bean(MetadataRecord.class);
		spark = SparkSession
			.builder()
			.appName(GenerateNativeStoreSparkJobTest.class.getSimpleName())
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	@Order(1)
	void testGenerateNativeStoreSparkJobRefresh() throws Exception {

		MDStoreVersion mdStoreV1 = prepareVersion("/eu/dnetlib/dhp/collection/mdStoreVersion_1.json");
		FileUtils.forceMkdir(new File(mdStoreV1.getHdfsPath()));

		IOUtils
			.copy(
				getClass().getResourceAsStream("/eu/dnetlib/dhp/collection/sequence_file"),
				new FileOutputStream(mdStoreV1.getHdfsPath() + "/sequence_file"));

		GenerateNativeStoreSparkJob
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-encoding", encoding,
					"-dateOfCollection", dateOfCollection,
					"-provenance", provenance,
					"-xpath", xpath,
					"-mdStoreVersion", OBJECT_MAPPER.writeValueAsString(mdStoreV1),
					"-readMdStoreVersion", "",
					"-workflowId", "abc"
				});

		verify(mdStoreV1);
	}

	@Test
	@Order(2)
	void testGenerateNativeStoreSparkJobIncremental() throws Exception {

		MDStoreVersion mdStoreV2 = prepareVersion("/eu/dnetlib/dhp/collection/mdStoreVersion_2.json");
		FileUtils.forceMkdir(new File(mdStoreV2.getHdfsPath()));

		IOUtils
			.copy(
				getClass().getResourceAsStream("/eu/dnetlib/dhp/collection/sequence_file"),
				new FileOutputStream(mdStoreV2.getHdfsPath() + "/sequence_file"));

		MDStoreVersion mdStoreV1 = prepareVersion("/eu/dnetlib/dhp/collection/mdStoreVersion_1.json");

		GenerateNativeStoreSparkJob
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-encoding", encoding,
					"-dateOfCollection", dateOfCollection,
					"-provenance", provenance,
					"-xpath", xpath,
					"-mdStoreVersion", OBJECT_MAPPER.writeValueAsString(mdStoreV2),
					"-readMdStoreVersion", OBJECT_MAPPER.writeValueAsString(mdStoreV1),
					"-workflowId", "abc"
				});

		verify(mdStoreV2);
	}

	@Test
	@Order(3)
	void testTransformSparkJob() throws Exception {

		setUpVocabulary();

		MDStoreVersion mdStoreV2 = prepareVersion("/eu/dnetlib/dhp/collection/mdStoreVersion_2.json");
		MDStoreVersion mdStoreCleanedVersion = prepareVersion("/eu/dnetlib/dhp/collection/mdStoreCleanedVersion.json");

		mockupTrasformationRule("simpleTRule", "/eu/dnetlib/dhp/transform/ext_simple.xsl");

		final Map<String, String> parameters = Stream.of(new String[][] {
			{
				"dateOfTransformation", "1234"
			},
			{
				"transformationPlugin", "XSLT_TRANSFORM"
			},
			{
				"transformationRuleId", "simpleTRule"
			},

		}).collect(Collectors.toMap(data -> data[0], data -> data[1]));

		TransformSparkJobNode
			.transformRecords(
				parameters, isLookUpService, spark, mdStoreV2.getHdfsPath() + MDSTORE_DATA_PATH,
				mdStoreCleanedVersion.getHdfsPath(), 200);

		final Encoder<MetadataRecord> encoder = Encoders.bean(MetadataRecord.class);
		final Dataset<MetadataRecord> mOutput = spark
			.read()
			.format("parquet")
			.load(mdStoreCleanedVersion.getHdfsPath() + MDSTORE_DATA_PATH)
			.as(encoder);

		final Long total = mOutput.count();

		final long recordTs = mOutput
			.filter((FilterFunction<MetadataRecord>) p -> p.getDateOfTransformation() == 1234)
			.count();

		final long recordNotEmpty = mOutput
			.filter((FilterFunction<MetadataRecord>) p -> !StringUtils.isBlank(p.getBody()))
			.count();

		assertEquals(total, recordTs);

		assertEquals(total, recordNotEmpty);

	}

	@Test
	void testJSONSerialization() throws Exception {
		final String s = IOUtils.toString(getClass().getResourceAsStream("mdStoreVersion_1.json"));
		System.out.println("s = " + s);
		final ObjectMapper mapper = new ObjectMapper();
		MDStoreVersion mi = mapper.readValue(s, MDStoreVersion.class);

		assertNotNull(mi);

	}

	@Test
	void testGenerationMetadataRecord() throws Exception {

		final String xml = IOUtils.toString(this.getClass().getResourceAsStream("./record.xml"));

		final MetadataRecord record = GenerateNativeStoreSparkJob
			.parseRecord(
				xml,
				"./*[local-name()='record']/*[local-name()='header']/*[local-name()='identifier']",
				"XML",
				new Provenance("foo", "bar", "ns_prefix"),
				System.currentTimeMillis(),
				null,
				null);

		assertNotNull(record.getId());
		assertNotNull(record.getOriginalId());
	}

	@Test
	void testEquals() throws IOException {

		final String xml = IOUtils.toString(this.getClass().getResourceAsStream("./record.xml"));
		final MetadataRecord record = GenerateNativeStoreSparkJob
			.parseRecord(
				xml,
				"./*[local-name()='record']/*[local-name()='header']/*[local-name()='identifier']",
				"XML",
				new Provenance("foo", "bar", "ns_prefix"),
				System.currentTimeMillis(),
				null,
				null);
		final MetadataRecord record1 = GenerateNativeStoreSparkJob
			.parseRecord(
				xml,
				"./*[local-name()='record']/*[local-name()='header']/*[local-name()='identifier']",
				"XML",
				new Provenance("foo", "bar", "ns_prefix"),
				System.currentTimeMillis(),
				null,
				null);

		record.setBody("ciao");
		record1.setBody("mondo");

		assertNotNull(record);
		assertNotNull(record1);
		assertEquals(record, record1);
	}

	protected void verify(MDStoreVersion mdStoreVersion) throws IOException {
		Assertions.assertTrue(new File(mdStoreVersion.getHdfsPath()).exists());

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		long seqFileSize = sc
			.sequenceFile(mdStoreVersion.getHdfsPath() + "/sequence_file", IntWritable.class, Text.class)
			.count();

		final Dataset<MetadataRecord> mdstore = spark.read().load(mdStoreVersion.getHdfsPath() + "/store").as(encoder);
		long mdStoreSize = mdstore.count();

		long declaredSize = Long.parseLong(IOUtils.toString(new FileReader(mdStoreVersion.getHdfsPath() + "/size")));

		Assertions.assertEquals(seqFileSize, declaredSize, "the size must be equal");
		Assertions.assertEquals(seqFileSize, mdStoreSize, "the size must be equal");

		long uniqueIds = mdstore
			.map((MapFunction<MetadataRecord, String>) MetadataRecord::getId, Encoders.STRING())
			.distinct()
			.count();

		Assertions.assertEquals(seqFileSize, uniqueIds, "the size must be equal");
	}

	public MDStoreVersion prepareVersion(String filename) throws IOException {
		MDStoreVersion mdstore = OBJECT_MAPPER
			.readValue(IOUtils.toString(getClass().getResource(filename)), MDStoreVersion.class);
		mdstore.setHdfsPath(String.format(mdstore.getHdfsPath(), workingDir.toString()));
		return mdstore;
	}

}
