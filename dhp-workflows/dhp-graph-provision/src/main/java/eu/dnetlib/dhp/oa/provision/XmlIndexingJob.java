
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lucidworks.spark.util.SolrSupport;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.provision.model.SerializableSolrInputDocument;
import eu.dnetlib.dhp.oa.provision.model.TupleWrapper;
import eu.dnetlib.dhp.oa.provision.utils.ISLookupClient;
import eu.dnetlib.dhp.oa.provision.utils.StreamingInputDocumentFactory;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.dhp.utils.saxon.SaxonTransformerFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import scala.Tuple2;

public class XmlIndexingJob {

	private static final Logger log = LoggerFactory.getLogger(XmlIndexingJob.class);

	public enum OutputFormat {
		SOLR, HDFS
	}

	private static final Integer DEFAULT_BATCH_SIZE = 1000;

	protected static final String DATE_FORMAT = "yyyy-MM-dd'T'hh:mm:ss'Z'";

	private final String inputPath;

	private final String format;

	private final int batchSize;

	private final OutputFormat outputFormat;

	private final String outputPath;

	private boolean shouldIndex;

	private final SparkSession spark;

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

		final String format = parser.get("format");
		log.info("format: {}", format);

		final String outputPath = Optional
			.ofNullable(parser.get("outputPath"))
			.map(StringUtils::trim)
			.orElse(null);
		log.info("outputPath: {}", outputPath);

		final Integer batchSize = Optional
			.ofNullable(parser.get("batchSize"))
			.map(Integer::valueOf)
			.orElse(DEFAULT_BATCH_SIZE);
		log.info("batchSize: {}", batchSize);

		final OutputFormat outputFormat = Optional
			.ofNullable(parser.get("outputFormat"))
			.map(OutputFormat::valueOf)
			.orElse(OutputFormat.SOLR);
		log.info("outputFormat: {}", outputFormat);

		final boolean shouldIndex = Optional
			.ofNullable(parser.get("shouldIndex"))
			.map(Boolean::valueOf)
			.orElse(false);
		log.info("shouldIndex: {}", shouldIndex);

		final SparkConf conf = new SparkConf();

		conf.registerKryoClasses(new Class[] {
			SerializableSolrInputDocument.class
		});

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				final String isLookupUrl = parser.get("isLookupUrl");
				log.info("isLookupUrl: {}", isLookupUrl);
				final ISLookupClient isLookup = new ISLookupClient(ISLookupClientFactory.getLookUpService(isLookupUrl));
				new XmlIndexingJob(spark, inputPath, format, batchSize, outputFormat, shouldIndex, outputPath)
					.run(isLookup);
			});
	}

	public XmlIndexingJob(SparkSession spark, String inputPath, String format, Integer batchSize,
		OutputFormat outputFormat, boolean shouldIndex, String outputPath) {
		this.spark = spark;
		this.inputPath = inputPath;
		this.format = format;
		this.batchSize = batchSize;
		this.outputFormat = outputFormat;
		this.shouldIndex = shouldIndex;
		this.outputPath = outputPath;
	}

	public void run(ISLookupClient isLookup) throws ISLookUpException, TransformerException {
		final String fields = isLookup.getLayoutSource(format);
		log.info("fields: {}", fields);

		final String xslt = isLookup.getLayoutTransformer();

		final String dsId = isLookup.getDsId(format);
		log.info("dsId: {}", dsId);

		final String zkHost = isLookup.getZkHost();
		log.info("zkHost: {}", zkHost);

		final String indexRecordXslt = getLayoutTransformer(format, fields, xslt);
		log.info("indexRecordTransformer {}", indexRecordXslt);

		final Encoder<TupleWrapper> encoder = Encoders.bean(TupleWrapper.class);
		final Dataset<TupleWrapper> records = spark
			.read()
			.schema(encoder.schema())
			.json(inputPath)
			.as(encoder)
			.map(
				(MapFunction<TupleWrapper, TupleWrapper>) t -> new TupleWrapper(
					toIndexRecord(SaxonTransformerFactory.newInstance(indexRecordXslt), t.getXml()),
					t.getJson()),
				Encoders.bean(TupleWrapper.class));

		switch (outputFormat) {
			case SOLR:
				if (shouldIndex) {
					final String collection = ProvisionConstants.getCollectionName(format);

					// SparkSolr >= 4
					// com.lucidworks.spark.BatchSizeType bt = com.lucidworks.spark.BatchSizeType.NUM_DOCS;
					// SolrSupport.indexDocs(zkHost, collection, batchSize, bt, docs.rdd());
					// SparkSolr < 4
					JavaRDD<SolrInputDocument> docs = records
						.javaRDD()
						.map(
							t -> new StreamingInputDocumentFactory().parseDocument(t.getXml(), t.getJson()));
					SolrSupport.indexDocs(zkHost, collection, batchSize, docs.rdd());
				}
				break;
			case HDFS:
				records
					.map(
						(MapFunction<TupleWrapper, SerializableSolrInputDocument>) t -> {
							SolrInputDocument s = new StreamingInputDocumentFactory()
								.parseDocument(t.getXml(), t.getJson());
							return new SerializableSolrInputDocument(s);
						},
						Encoders.kryo(SerializableSolrInputDocument.class))
					.write()
					.mode(SaveMode.Overwrite)
					.parquet(outputPath);
				break;
			default:
				throw new IllegalArgumentException("invalid outputFormat: " + outputFormat);
		}
	}

	protected static String toIndexRecord(Transformer tr, final String xmlRecord) {
		final StreamResult res = new StreamResult(new StringWriter());
		try {
			tr.transform(new StreamSource(new StringReader(xmlRecord)), res);
			return res.getWriter().toString();
		} catch (TransformerException e) {
			throw new IllegalArgumentException("XPathException on record: \n" + xmlRecord, e);
		}
	}

	/**
	 * Creates the XSLT responsible for building the index xml records.
	 *
	 * @param format Metadata format name (DMF|TMF)
	 * @param xslt xslt for building the index record transformer
	 * @param fields the list of fields
	 * @return the javax.xml.transform.Transformer
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

}
