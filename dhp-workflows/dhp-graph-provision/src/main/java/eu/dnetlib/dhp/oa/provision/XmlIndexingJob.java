
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.provision.model.SerializableSolrInputDocument;
import eu.dnetlib.dhp.oa.provision.model.TupleWrapper;
import eu.dnetlib.dhp.oa.provision.utils.ISLookupClient;
import eu.dnetlib.dhp.oa.provision.utils.StreamingInputDocumentFactory;
import eu.dnetlib.dhp.schema.solr.*;
import eu.dnetlib.dhp.sparksolr.DHPSolrSupport;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.dhp.utils.saxon.SaxonTransformerFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;

public class XmlIndexingJob extends AbstractSolrRecordTransformJob {

	private static final Logger log = LoggerFactory.getLogger(XmlIndexingJob.class);

	private static final Integer DEFAULT_BATCH_SIZE = 1000;

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

	private final String inputPath;

	private final String format;

	private final String shadowCollection;

	private final int batchSize;

	private final Boolean shouldFilterXmlPayload;

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

		final Boolean shouldFilterXmlPayload = Optional
			.ofNullable(parser.get("shouldFilterXmlPayload"))
			.map(Boolean::valueOf)
			.orElse(Boolean.FALSE);
		log.info("shouldFilterXmlPayload: {}", shouldFilterXmlPayload);

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
				new XmlIndexingJob(spark, inputPath, shadowFormat, shadowCollection, batchSize, shouldFilterXmlPayload)
					.run(isLookup);
			});
	}

	public XmlIndexingJob(SparkSession spark, String inputPath, String format, String shadowCollection,
		Integer batchSize, Boolean shouldFilterXmlPayload) {
		this.spark = spark;
		this.inputPath = inputPath;
		this.format = format;
		this.shadowCollection = shadowCollection;
		this.batchSize = batchSize;
		this.shouldFilterXmlPayload = shouldFilterXmlPayload;
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

		final boolean serializableShouldFilterXmlPayload = Boolean.valueOf(shouldFilterXmlPayload);

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
				t -> {
					final SolrInputDocument doc = new StreamingInputDocumentFactory()
						.parseDocument(t.getXml(), t.getJson());
					return filterXmlPayload(doc, serializableShouldFilterXmlPayload);
				});

		DHPSolrSupport.indexDocs(zkHost, shadowCollection, batchSize, docs.rdd());
	}

	private static SolrInputDocument filterXmlPayload(SolrInputDocument doc, Boolean shouldFilterXmlPayload) {
		if (Boolean.TRUE.equals(shouldFilterXmlPayload)) {
			String json = (String) doc.get(StreamingInputDocumentFactory.INDEX_JSON_RESULT).getFirstValue();
			try {
				SolrRecord sr = OBJECT_MAPPER.readValue(json, SolrRecord.class);
				boolean isProject = RecordType.project.equals(sr.getHeader().getRecordType());
				boolean isRelatedToEcFunding = Optional
					.ofNullable(sr.getLinks())
					.map(
						links -> links
							.stream()
							.anyMatch(
								rr -> {
									final boolean isRelatedProject = RecordType.project
										.equals(rr.getHeader().getRelatedRecordType());
									final String funderShortName = Optional
										.ofNullable(rr.getFunding())
										.map(Funding::getFunder)
										.map(Funder::getShortname)
										.orElse("");
									return isRelatedProject && "EC".equals(funderShortName);
								}))
					.orElse(false);
				if (!isProject || !isRelatedToEcFunding) {
					doc.remove(StreamingInputDocumentFactory.INDEX_RESULT);
				}
			} catch (IOException e) {
				log.error("Error mapping json to SolrRecord", e);
				throw new IllegalStateException(e);
			}
		}
		return doc;
	}

}
