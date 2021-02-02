
package eu.dnetlib.dhp.transformation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.dhp.aggregation.common.AggregationCounter;
import eu.dnetlib.dhp.collection.CollectionJobTest;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.model.mdstore.MetadataRecord;
import eu.dnetlib.dhp.transformation.xslt.XSLTTransformationFunction;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
public class TransformationJobTest {

	private static SparkSession spark;

	@Mock
	private ISLookUpService isLookUpService;

	private VocabularyGroup vocabularies;

	@BeforeEach
	public void setUp() throws ISLookUpException, IOException {
		lenient().when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARIES_XQUERY)).thenReturn(vocs());

		lenient()
			.when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARY_SYNONYMS_XQUERY))
			.thenReturn(synonyms());
		vocabularies = VocabularyGroup.loadVocsFromIS(isLookUpService);
	}

	@BeforeAll
	public static void beforeAll() {
		SparkConf conf = new SparkConf();
		conf.setAppName(CollectionJobTest.class.getSimpleName());
		conf.setMaster("local");
		spark = SparkSession.builder().config(conf).getOrCreate();
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
		mr.setBody(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/input.xml")));

		// We Load the XSLT transformation Rule from the classpath
		XSLTTransformationFunction tr = loadTransformationRule("/eu/dnetlib/dhp/transform/ext_simple.xsl");

		// Print the record
		System.out.println(tr.call(mr).getBody());
		// TODO Create significant Assert

	}

	@DisplayName("Test TransformSparkJobNode.main")
	@Test
	public void transformTest(@TempDir Path testDir) throws Exception {

		final String mdstore_input = this.getClass().getResource("/eu/dnetlib/dhp/transform/mdstorenative").getFile();
		final String mdstore_output = testDir.toString() + "/version";

		mockupTrasformationRule("simpleTRule", "/eu/dnetlib/dhp/transform/ext_simple.xsl");

//		final String arguments = "-issm true -i %s -o %s -d 1 -w 1 -tp XSLT_TRANSFORM -tr simpleTRule";

		final Map<String, String> parameters = Stream.of(new String[][] {
			{
				"dateOfTransformation", "1234"
			},
			{
				"transformationPlugin", "XSLT_TRANSFORM"
			},
			{
				"transformationRuleTitle", "simpleTRule"
			},

		}).collect(Collectors.toMap(data -> data[0], data -> data[1]));

		TransformSparkJobNode.transformRecords(parameters, isLookUpService, spark, mdstore_input, mdstore_output);

		// TODO introduce useful assertions

		final Encoder<MetadataRecord> encoder = Encoders.bean(MetadataRecord.class);
		final Dataset<MetadataRecord> mOutput = spark.read().format("parquet").load(mdstore_output).as(encoder);

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

	private void mockupTrasformationRule(final String trule, final String path) throws Exception {
		final String trValue = IOUtils.toString(this.getClass().getResourceAsStream(path));

		lenient()
			.when(isLookUpService.quickSearchProfile(String.format(TransformationFactory.TRULE_XQUERY, trule)))
			.thenReturn(Collections.singletonList(trValue));
	}

	private XSLTTransformationFunction loadTransformationRule(final String path) throws Exception {
		final String trValue = IOUtils.toString(this.getClass().getResourceAsStream(path));
		final LongAccumulator la = new LongAccumulator();
		return new XSLTTransformationFunction(new AggregationCounter(la, la, la), trValue, 0, vocabularies);
	}

	private List<String> vocs() throws IOException {
		return IOUtils
			.readLines(TransformationJobTest.class.getResourceAsStream("/eu/dnetlib/dhp/transform/terms.txt"));
	}

	private List<String> synonyms() throws IOException {
		return IOUtils
			.readLines(TransformationJobTest.class.getResourceAsStream("/eu/dnetlib/dhp/transform/synonyms.txt"));
	}
}
