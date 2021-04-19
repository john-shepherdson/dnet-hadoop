
package eu.dnetlib.dhp.transformation;

import static eu.dnetlib.dhp.common.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.utils.DHPUtils.*;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.data.mdstore.manager.common.model.MDStoreVersion;
import eu.dnetlib.dhp.aggregation.common.AggregationCounter;
import eu.dnetlib.dhp.aggregation.common.AggregatorReport;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.message.MessageSender;
import eu.dnetlib.dhp.schema.mdstore.MetadataRecord;
import eu.dnetlib.dhp.transformation.xslt.XSLTTransformationFunction;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class TransformSparkJobNode {

	private static final Logger log = LoggerFactory.getLogger(TransformSparkJobNode.class);

	private static int RECORDS_PER_TASK = 200;

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					TransformSparkJobNode.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/transformation/transformation_input_parameters.json")));

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String mdstoreInputVersion = parser.get("mdstoreInputVersion");
		final String mdstoreOutputVersion = parser.get("mdstoreOutputVersion");

		final MDStoreVersion nativeMdStoreVersion = MAPPER.readValue(mdstoreInputVersion, MDStoreVersion.class);
		final String inputPath = nativeMdStoreVersion.getHdfsPath() + MDSTORE_DATA_PATH;
		log.info("inputPath: {}", inputPath);

		final MDStoreVersion cleanedMdStoreVersion = MAPPER.readValue(mdstoreOutputVersion, MDStoreVersion.class);
		final String outputBasePath = cleanedMdStoreVersion.getHdfsPath();
		log.info("outputBasePath: {}", outputBasePath);

		final String isLookupUrl = parser.get("isLookupUrl");
		log.info(String.format("isLookupUrl: %s", isLookupUrl));

		final String dateOfTransformation = parser.get("dateOfTransformation");
		log.info(String.format("dateOfTransformation: %s", dateOfTransformation));

		final Integer rpt = Optional
			.ofNullable(parser.get("recordsPerTask"))
			.map(Integer::valueOf)
			.orElse(RECORDS_PER_TASK);

		final ISLookUpService isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl);

		final VocabularyGroup vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService);

		log.info("Retrieved {} vocabularies", vocabularies.vocabularyNames().size());

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				transformRecords(
					parser.getObjectMap(), isLookupService, spark, inputPath, outputBasePath, rpt);
			});
	}

	public static void transformRecords(final Map<String, String> args, final ISLookUpService isLookUpService,
		final SparkSession spark, final String inputPath, final String outputBasePath, final Integer rpt)
		throws DnetTransformationException, IOException {

		final LongAccumulator totalItems = spark.sparkContext().longAccumulator(CONTENT_TOTALITEMS);
		final LongAccumulator errorItems = spark.sparkContext().longAccumulator(CONTENT_INVALIDRECORDS);
		final LongAccumulator transformedItems = spark.sparkContext().longAccumulator(CONTENT_TRANSFORMEDRECORDS);
		final AggregationCounter ct = new AggregationCounter(totalItems, errorItems, transformedItems);
		final Encoder<MetadataRecord> encoder = Encoders.bean(MetadataRecord.class);

		final String dnetMessageManagerURL = args.get(DNET_MESSAGE_MGR_URL);
		log.info("dnetMessageManagerURL is {}", dnetMessageManagerURL);

		final String workflowId = args.get("workflowId");
		log.info("workflowId is {}", workflowId);

		MapFunction<MetadataRecord, MetadataRecord> x = TransformationFactory
				.getTransformationPlugin(args, ct, isLookUpService);

		final Dataset<MetadataRecord> inputMDStore = spark
			.read()
			.format("parquet")
			.load(inputPath)
			.as(encoder);

		final long totalInput = inputMDStore.count();

		final MessageSender messageSender = new MessageSender(dnetMessageManagerURL, workflowId);
		try (AggregatorReport report = new AggregatorReport(messageSender)) {
			try {
				JavaRDD<MetadataRecord> mdstore = inputMDStore
					.javaRDD()
					.repartition(getRepartitionNumber(totalInput, rpt))
					.map((Function<MetadataRecord, MetadataRecord>) x::call);
				saveDataset(spark.createDataset(mdstore.rdd(), encoder), outputBasePath + MDSTORE_DATA_PATH);

				log.info("Transformed item " + ct.getProcessedItems().count());
				log.info("Total item " + ct.getTotalItems().count());
				log.info("Transformation Error item " + ct.getErrorItems().count());

				final long mdStoreSize = spark.read().load(outputBasePath + MDSTORE_DATA_PATH).count();
				writeHdfsFile(
					spark.sparkContext().hadoopConfiguration(),
					"" + mdStoreSize, outputBasePath + MDSTORE_SIZE_PATH);
			} catch (Throwable e) {
				log.error("error during record transformation", e);
				report.put(TransformSparkJobNode.class.getSimpleName(), e.getMessage());
				report.put(CONTENT_TOTALITEMS, ct.getTotalItems().value().toString());
				report.put(CONTENT_INVALIDRECORDS, ct.getErrorItems().value().toString());
				report.put(CONTENT_TRANSFORMEDRECORDS, ct.getProcessedItems().value().toString());
				throw e;
			}
		}
	}

}
