
package eu.dnetlib.dhp.transformation;

import static eu.dnetlib.dhp.common.Constants.MDSTORE_DATA_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.dhp.aggregation.AbstractVocabularyTest;
import eu.dnetlib.dhp.aggregation.common.AggregationCounter;
import eu.dnetlib.dhp.schema.mdstore.MetadataRecord;
import eu.dnetlib.dhp.schema.mdstore.Provenance;
import eu.dnetlib.dhp.transformation.xslt.DateCleaner;
import eu.dnetlib.dhp.transformation.xslt.XSLTTransformationFunction;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;

@ExtendWith(MockitoExtension.class)
class TransformationJobTest extends AbstractVocabularyTest {

	private SparkConf sparkConf;

	@BeforeEach
	public void setUp() throws IOException, ISLookUpException {
		setUpVocabulary();

		sparkConf = new SparkConf();
		sparkConf.setMaster("local[*]");
		sparkConf.set("spark.driver.host", "localhost");
		sparkConf.set("spark.ui.enabled", "false");
	}

	@Test
	@DisplayName("Test Date cleaner")
	void testDateCleaner() throws Exception {
		final DateCleaner dc = new DateCleaner();
		assertEquals("1982-09-20", dc.clean("20/09/1982"));
		assertEquals("2002-09-20", dc.clean("20-09-2002"));
		assertEquals("2002-09-20", dc.clean("2002-09-20"));
		assertEquals("2002-09-01", dc.clean("2002-9"));
		assertEquals("2021-01-01", dc.clean("2021"));
	}

	@Test
	@DisplayName("Test Transform Single XML using zenodo_tr XSLTTransformator")
	void testTransformSaxonHE() throws Exception {

		// We Set the input Record getting the XML from the classpath
		final MetadataRecord mr = new MetadataRecord();

		mr.setProvenance(new Provenance("DSID", "DSNAME", "PREFIX"));
		mr.setBody(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/input_zenodo.xml")));
		// We Load the XSLT transformation Rule from the classpath
		final XSLTTransformationFunction tr = loadTransformationRule("/eu/dnetlib/dhp/transform/zenodo_tr.xslt");

		final MetadataRecord result = tr.call(mr);

		// Print the record
		System.out.println(result.getBody());
		// TODO Create significant Assert
	}

	@Test
	@DisplayName("Test Transform Inst.&Them.v4 record XML with zenodo_tr")
	void testTransformITGv4Zenodo() throws Exception {

		// We Set the input Record getting the XML from the classpath
		final MetadataRecord mr = new MetadataRecord();
		mr.setProvenance(new Provenance("DSID", "DSNAME", "PREFIX"));
		mr.setBody(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/input_itgv4.xml")));
		// We Load the XSLT transformation Rule from the classpath
		final XSLTTransformationFunction tr = loadTransformationRule("/eu/dnetlib/dhp/transform/zenodo_tr.xslt");

		final MetadataRecord result = tr.call(mr);

		// Print the record
		System.out.println(result.getBody());
		// TODO Create significant Assert
	}

	@Test
	@DisplayName("Test Transform record XML with xslt_cleaning_datarepo_datacite/oaiOpenAIRE")
	void testTransformMostlyUsedScript() throws Exception {

		String xslTransformationScript = "";
		xslTransformationScript = "/eu/dnetlib/dhp/transform/scripts/xslt_cleaning_datarepo_datacite.xsl";
		xslTransformationScript = "/eu/dnetlib/dhp/transform/scripts/xslt_cleaning_oaiOpenaire_datacite_ExchangeLandingpagePid.xsl";

		// We Set the input Record getting the XML from the classpath
		final MetadataRecord mr = new MetadataRecord();
		mr.setProvenance(new Provenance("DSID", "DSNAME", "PREFIX"));
		mr.setBody(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/input_itgv4.xml")));
		// We Load the XSLT transformation Rule from the classpath
		final XSLTTransformationFunction tr = loadTransformationRule(xslTransformationScript);

		final MetadataRecord result = tr.call(mr);

		// Print the record
		System.out.println(result.getBody());
		// TODO Create significant Assert
	}

	@Test
	@DisplayName("Test Transform record XML with xslt_cleaning_REST_OmicsDI")
	void testTransformRestScript() throws Exception {

		String xslTransformationScript = "";
		xslTransformationScript = "/eu/dnetlib/dhp/transform/scripts/xslt_cleaning_REST_OmicsDI.xsl";

		// We Set the input Record getting the XML from the classpath
		final MetadataRecord mr = new MetadataRecord();
		mr.setProvenance(new Provenance("DSID", "DSNAME", "PREFIX"));
		mr.setBody(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/input_omicsdi.xml")));
		// We Load the XSLT transformation Rule from the classpath
		final XSLTTransformationFunction tr = loadTransformationRule(xslTransformationScript);

		final MetadataRecord result = tr.call(mr);

		// Print the record
		System.out.println(result.getBody());
		// TODO Create significant Assert
	}

	@Test
	@DisplayName("Test TransformSparkJobNode.main with oaiOpenaire_datacite (v4)")
	void transformTestITGv4OAIdatacite(@TempDir
	final Path testDir) throws Exception {

		try (SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate()) {

			final String mdstore_input = this
				.getClass()
				.getResource("/eu/dnetlib/dhp/transform/mdstorenative")
				.getFile();
			final String mdstore_output = testDir.toString() + "/version";

			mockupTrasformationRule(
				"simpleTRule",
				"/eu/dnetlib/dhp/transform/scripts/xslt_cleaning_oaiOpenaire_datacite_ExchangeLandingpagePid.xsl");

			final Map<String, String> parameters = Stream.of(new String[][] {
				{
					"dateOfTransformation", "1234"
				},
				{
					"varOfficialName", "Publications at Bielefeld University"
				},
				{
					"varOfficialId", "opendoar____::2294"
				},
				{
					"transformationPlugin", "XSLT_TRANSFORM"
				},
				{
					"transformationRuleId", "simpleTRule"
				},

			}).collect(Collectors.toMap(data -> data[0], data -> data[1]));

			TransformSparkJobNode
				.transformRecords(parameters, isLookUpService, spark, mdstore_input, mdstore_output, 200);

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
	}

	@Test
	@DisplayName("Test TransformSparkJobNode.main")
	void transformTest(@TempDir
	final Path testDir) throws Exception {

		try (SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate()) {

			final String mdstore_input = this
				.getClass()
				.getResource("/eu/dnetlib/dhp/transform/mdstorenative")
				.getFile();
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

			TransformSparkJobNode
				.transformRecords(parameters, isLookUpService, spark, mdstore_input, mdstore_output, 200);

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
	}

	@Test
	@DisplayName("Test Transform Single XML using cnr_explora_tr XSLTTransformator")
	void testCnrExploraTransformSaxonHE() throws Exception {

		// We Set the input Record getting the XML from the classpath
		final MetadataRecord mr = new MetadataRecord();

		mr.setProvenance(new Provenance("openaire____::cnr_explora", "CNR ExploRA", "cnr_________"));
		mr.setBody(IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/transform/input_cnr_explora.xml")));
		// We Load the XSLT transformation Rule from the classpath
		final XSLTTransformationFunction tr = loadTransformationRule("/eu/dnetlib/dhp/transform/cnr_explora_tr.xslt");

		final MetadataRecord result = tr.call(mr);

		// Print the record
		System.out.println(result.getBody());
		// TODO Create significant Assert
	}

	private XSLTTransformationFunction loadTransformationRule(final String path) throws Exception {
		final String trValue = IOUtils.toString(this.getClass().getResourceAsStream(path));
		final LongAccumulator la = new LongAccumulator();
		return new XSLTTransformationFunction(new AggregationCounter(la, la, la), trValue, 0, vocabularies);
	}

}
