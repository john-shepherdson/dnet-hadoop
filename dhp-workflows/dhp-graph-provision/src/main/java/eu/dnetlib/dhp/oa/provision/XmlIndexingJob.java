
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import javax.xml.transform.TransformerException;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lucidworks.spark.util.SolrSupport;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.provision.model.SerializableSolrInputDocument;
import eu.dnetlib.dhp.oa.provision.model.TupleWrapper;
import eu.dnetlib.dhp.oa.provision.utils.ISLookupClient;
import eu.dnetlib.dhp.oa.provision.utils.StreamingInputDocumentFactory;
import eu.dnetlib.dhp.sparksolr.DHPSolrSupport;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.dhp.utils.saxon.SaxonTransformerFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;

public class XmlIndexingJob extends AbstractSolrRecordTransformJob {

	private static final Logger log = LoggerFactory.getLogger(XmlIndexingJob.class);

	private static final Integer DEFAULT_BATCH_SIZE = 1000;

	private final String inputPath;

	private final String format;

	private final String shadowCollection;

	private final int batchSize;

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

		final String shadowFormat = parser.get("shadowFormat");
		log.info("shadowFormat: {}", shadowFormat);

		final String shadowCollection = ProvisionConstants.getCollectionName(shadowFormat);
		log.info("shadowCollection: {}", shadowCollection);

		final Integer batchSize = Optional
			.ofNullable(parser.get("batchSize"))
			.map(Integer::valueOf)
			.orElse(DEFAULT_BATCH_SIZE);
		log.info("batchSize: {}", batchSize);

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
				new XmlIndexingJob(spark, inputPath, shadowFormat, shadowCollection, batchSize)
					.run(isLookup);
			});
	}

	public XmlIndexingJob(SparkSession spark, String inputPath, String format, String shadowCollection,
		Integer batchSize) {
		this.spark = spark;
		this.inputPath = inputPath;
		this.format = format;
		this.shadowCollection = shadowCollection;
		this.batchSize = batchSize;
	}

	public void run(ISLookupClient isLookup) throws ISLookUpException, TransformerException {
		final String fields = isLookup.getLayoutSource(format);
		log.info("fields: {}", fields);

		final String xslt = isLookup.getLayoutTransformer();

		final String zkHost = isLookup.getZkHost();
		log.info("zkHost: {}", zkHost);

		final String indexRecordXslt = getLayoutTransformer(format, fields, xslt);
		log.info("indexRecordTransformer {}", indexRecordXslt);

		final Encoder<TupleWrapper> encoder = Encoders.bean(TupleWrapper.class);

		JavaRDD<SolrInputDocument> docs = spark
			.read()
			.schema(encoder.schema())
			.json(inputPath)
			.as(encoder)
			.map(
				(MapFunction<TupleWrapper, TupleWrapper>) t -> new TupleWrapper(
					toIndexRecord(SaxonTransformerFactory.newInstance(indexRecordXslt), t.getXml()),
					t.getJson()),
				Encoders.bean(TupleWrapper.class))
			.javaRDD()
			.map(
				t -> new StreamingInputDocumentFactory().parseDocument(t.getXml(), t.getJson()));
		DHPSolrSupport.indexDocs(zkHost, shadowCollection, batchSize, docs.rdd());
	}

}
