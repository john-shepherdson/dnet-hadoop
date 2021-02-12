
package eu.dnetlib.dhp.transformation;

import static eu.dnetlib.dhp.common.Constants.MDSTORE_DATA_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.dhp.aggregation.AbstractVocabularyTest;
import eu.dnetlib.dhp.aggregation.common.AggregationCounter;
import eu.dnetlib.dhp.collection.GenerateNativeStoreSparkJobTest;
import eu.dnetlib.dhp.model.mdstore.MetadataRecord;
import eu.dnetlib.dhp.transformation.xslt.XSLTTransformationFunction;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;

@ExtendWith(MockitoExtension.class)
public class TransformationJobTest extends AbstractVocabularyTest {

	private static SparkSession spark;

	@BeforeAll
	public static void beforeAll() throws IOException, ISLookUpException {
		SparkConf conf = new SparkConf();
		conf.setAppName(GenerateNativeStoreSparkJobTest.class.getSimpleName());
		conf.setMaster("local");
		spark = SparkSession.builder().config(conf).getOrCreate();
	}

	@BeforeEach
	public void setUp() throws IOException, ISLookUpException {
		setUpVocabulary();
	}

	@AfterAll
	public static void afterAll() {
		spark.stop();
	}

	@Test
	@DisplayName("Test Transform Single XML using XSLTTransformator")
	public void testTransformSaxonHE() throws Exception {

		// We Set the input Record getting the XML from the classpath
		final MetadataRecord mr = new MetadataRecord();
		mr.setBody(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/input_zenodo.xml")));

		// We Load the XSLT transformation Rule from the classpath
		XSLTTransformationFunction tr = loadTransformationRule("/eu/dnetlib/dhp/transform/zenodo_tr.xslt");


		MetadataRecord result = tr.call(mr);




		// Print the record
		System.out.println(result.getBody());
		// TODO Create significant Assert

	}

	@DisplayName("Test TransformSparkJobNode.main")
	@Test
	public void transformTest(@TempDir Path testDir) throws Exception {

		final String mdstore_input = this.getClass().getResource("/eu/dnetlib/dhp/transform/mdstorenative").getFile();
		final String mdstore_output = testDir.toString() + "/version";

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

		TransformSparkJobNode.transformRecords(parameters, isLookUpService, spark, mdstore_input, mdstore_output);

		// TODO introduce useful assertions

		final Encoder<MetadataRecord> encoder = Encoders.bean(MetadataRecord.class);
		final Dataset<MetadataRecord> mOutput = spark
			.read()
			.format("parquet")
			.load(mdstore_output + MDSTORE_DATA_PATH)
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
	public void tryLoadFolderOnCP() throws Exception {
		final String path = this.getClass().getResource("/eu/dnetlib/dhp/transform/mdstorenative").getFile();
		System.out.println("path = " + path);

		Path tempDirWithPrefix = Files.createTempDirectory("mdstore_output");

		System.out.println(tempDirWithPrefix.toFile().getAbsolutePath());

		Files.deleteIfExists(tempDirWithPrefix);
	}

	private XSLTTransformationFunction loadTransformationRule(final String path) throws Exception {
		final String trValue = IOUtils.toString(this.getClass().getResourceAsStream(path));
		final LongAccumulator la = new LongAccumulator();
		return new XSLTTransformationFunction(new AggregationCounter(la, la, la), trValue, 0, vocabularies);
	}

}
