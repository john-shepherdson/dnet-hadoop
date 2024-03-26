
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import javax.xml.transform.TransformerException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.provision.model.SerializableSolrInputDocument;
import eu.dnetlib.dhp.oa.provision.model.TupleWrapper;
import eu.dnetlib.dhp.oa.provision.utils.ISLookupClient;
import eu.dnetlib.dhp.oa.provision.utils.StreamingInputDocumentFactory;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.dhp.utils.saxon.SaxonTransformerFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;

public class SolrRecordDumpJob extends AbstractSolrRecordTransformJob {

	private static final Logger log = LoggerFactory.getLogger(SolrRecordDumpJob.class);

	private static final Integer DEFAULT_BATCH_SIZE = 1000;

	private final String inputPath;

	private final String format;

	private final String outputPath;

	private final SparkSession spark;

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SolrRecordDumpJob.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/provision/input_params_solr_record_dump.json")));
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
				new SolrRecordDumpJob(spark, inputPath, format, outputPath).run(isLookup);
			});
	}

	public SolrRecordDumpJob(SparkSession spark, String inputPath, String format, String outputPath) {
		this.spark = spark;
		this.inputPath = inputPath;
		this.format = format;
		this.outputPath = outputPath;
	}

	public void run(ISLookupClient isLookup) throws ISLookUpException, TransformerException {
		final String fields = isLookup.getLayoutSource(format);
		log.info("fields: {}", fields);

		final String xslt = isLookup.getLayoutTransformer();

		final String dsId = isLookup.getDsId(format);
		log.info("dsId: {}", dsId);

		final String indexRecordXslt = getLayoutTransformer(format, fields, xslt);
		log.info("indexRecordTransformer {}", indexRecordXslt);

		final Encoder<TupleWrapper> encoder = Encoders.bean(TupleWrapper.class);
		spark
			.read()
			.schema(encoder.schema())
			.json(inputPath)
			.as(encoder)
			.map(
				(MapFunction<TupleWrapper, TupleWrapper>) t -> new TupleWrapper(
					toIndexRecord(SaxonTransformerFactory.newInstance(indexRecordXslt), t.getXml()),
					t.getJson()),
				Encoders.bean(TupleWrapper.class))
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
	}

}
