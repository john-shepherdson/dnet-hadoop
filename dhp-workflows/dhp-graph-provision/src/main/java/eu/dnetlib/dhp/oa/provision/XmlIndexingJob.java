
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lucidworks.spark.util.SolrSupport;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.provision.utils.StreamingInputDocumentFactory;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.dhp.utils.saxon.SaxonTransformerFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpDocumentNotFoundException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class XmlIndexingJob {

	private static final Logger log = LoggerFactory.getLogger(XmlIndexingJob.class);

	private static final Integer DEFAULT_BATCH_SIZE = 1000;

	private static final String LAYOUT = "index";
	private static final String INTERPRETATION = "openaire";
	private static final String SEPARATOR = "-";
	public static final String DATE_FORMAT = "yyyy-MM-dd'T'hh:mm:ss'Z'";

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					XmlIndexingJob.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/provision/input_params_update_index.json")));
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		final String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		final String format = parser.get("format");
		log.info("format: {}", format);

		final Integer batchSize = parser.getObjectMap().containsKey("batchSize")
			? Integer.valueOf(parser.get("batchSize"))
			: DEFAULT_BATCH_SIZE;
		log.info("batchSize: {}", batchSize);

		final ISLookUpService isLookup = ISLookupClientFactory.getLookUpService(isLookupUrl);
		final String fields = getLayoutSource(isLookup, format);
		log.info("fields: {}", fields);

		final String xslt = getLayoutTransformer(isLookup);

		final String dsId = getDsId(format, isLookup);
		log.info("dsId: {}", dsId);

		final String zkHost = getZkHost(isLookup);
		log.info("zkHost: {}", zkHost);

		final String version = getRecordDatestamp();

		final String indexRecordXslt = getLayoutTransformer(format, fields, xslt);
		log.info("indexRecordTransformer {}", indexRecordXslt);

		final SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

				RDD<SolrInputDocument> docs = sc
					.sequenceFile(inputPath, Text.class, Text.class)
					.map(t -> t._2().toString())
					.map(s -> toIndexRecord(SaxonTransformerFactory.newInstance(indexRecordXslt), s))
					.map(s -> new StreamingInputDocumentFactory(version, dsId).parseDocument(s))
					.rdd();

				final String collection = format + SEPARATOR + LAYOUT + SEPARATOR + INTERPRETATION;
				SolrSupport.indexDocs(zkHost, collection, batchSize, docs);
			});
	}

	protected static String toIndexRecord(Transformer tr, final String record) {
		final StreamResult res = new StreamResult(new StringWriter());
		try {
			tr.transform(new StreamSource(new StringReader(record)), res);
			return res.getWriter().toString();
		} catch (Throwable e) {
			log.error("XPathException on record: \n {}", record, e);
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * Creates the XSLT responsible for building the index xml records.
	 *
	 * @param format Metadata format name (DMF|TMF)
	 * @param xslt xslt for building the index record transformer
	 * @param fields the list of fields
	 * @return the javax.xml.transform.Transformer
	 * @throws ISLookUpException could happen
	 * @throws IOException could happen
	 * @throws TransformerException could happen
	 */
	protected static String getLayoutTransformer(String format, String fields, String xslt)
		throws TransformerException {

		final Transformer layoutTransformer = SaxonTransformerFactory.newInstance(xslt);
		final StreamResult layoutToXsltXslt = new StreamResult(new StringWriter());

		layoutTransformer.setParameter("format", format);
		layoutTransformer.transform(new StreamSource(new StringReader(fields)), layoutToXsltXslt);

		return layoutToXsltXslt.getWriter().toString();
	}

	/**
	 * method return a solr-compatible string representation of a date, used to mark all records as indexed today
	 *
	 * @return the parsed date
	 */
	public static String getRecordDatestamp() {
		return new SimpleDateFormat(DATE_FORMAT).format(new Date());
	}

	/**
	 * Method retrieves from the information system the list of fields associated to the given MDFormat name
	 *
	 * @param isLookup the ISLookup service stub
	 * @param format the Metadata format name
	 * @return the string representation of the list of fields to be indexed
	 * @throws ISLookUpDocumentNotFoundException
	 * @throws ISLookUpException
	 */
	private static String getLayoutSource(final ISLookUpService isLookup, final String format)
		throws ISLookUpDocumentNotFoundException, ISLookUpException {
		return doLookup(
			isLookup,
			String
				.format(
					"collection('')//RESOURCE_PROFILE[.//RESOURCE_TYPE/@value = 'MDFormatDSResourceType' and .//NAME='%s']//LAYOUT[@name='%s']",
					format, LAYOUT));
	}

	/**
	 * Method retrieves from the information system the openaireLayoutToRecordStylesheet
	 *
	 * @param isLookup the ISLookup service stub
	 * @return the string representation of the XSLT contained in the transformation rule profile
	 * @throws ISLookUpDocumentNotFoundException
	 * @throws ISLookUpException
	 */
	private static String getLayoutTransformer(ISLookUpService isLookup) throws ISLookUpException {
		return doLookup(
			isLookup,
			"collection('/db/DRIVER/TransformationRuleDSResources/TransformationRuleDSResourceType')"
				+ "//RESOURCE_PROFILE[./BODY/CONFIGURATION/SCRIPT/TITLE/text() = 'openaireLayoutToRecordStylesheet']//CODE/node()");
	}

	/**
	 * Method retrieves from the information system the IndexDS profile ID associated to the given MDFormat name
	 *
	 * @param format
	 * @param isLookup
	 * @return the IndexDS identifier
	 * @throws ISLookUpException
	 */
	private static String getDsId(String format, ISLookUpService isLookup) throws ISLookUpException {
		return doLookup(
			isLookup,
			String
				.format(
					"collection('/db/DRIVER/IndexDSResources/IndexDSResourceType')"
						+ "//RESOURCE_PROFILE[./BODY/CONFIGURATION/METADATA_FORMAT/text() = '%s']//RESOURCE_IDENTIFIER/@value/string()",
					format));
	}

	/**
	 * Method retrieves from the information system the zookeeper quorum of the Solr server
	 *
	 * @param isLookup
	 * @return the zookeeper quorum of the Solr server
	 * @throws ISLookUpException
	 */
	private static String getZkHost(ISLookUpService isLookup) throws ISLookUpException {
		return doLookup(
			isLookup,
			"for $x in /RESOURCE_PROFILE[.//RESOURCE_TYPE/@value='IndexServiceResourceType'] return $x//PROTOCOL[./@name='solr']/@address/string()");
	}

	private static String doLookup(ISLookUpService isLookup, String xquery) throws ISLookUpException {
		log.info(String.format("running xquery: %s", xquery));
		final String res = isLookup.getResourceProfileByQuery(xquery);
		log.info(String.format("got response (100 chars): %s", StringUtils.left(res, 100) + " ..."));
		return res;
	}
}
