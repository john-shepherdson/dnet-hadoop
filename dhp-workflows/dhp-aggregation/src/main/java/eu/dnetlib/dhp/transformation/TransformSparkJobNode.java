
package eu.dnetlib.dhp.transformation;

import static eu.dnetlib.dhp.aggregation.common.AggregationConstants.*;
import static eu.dnetlib.dhp.aggregation.common.AggregationUtility.saveDataset;
import static eu.dnetlib.dhp.aggregation.common.AggregationUtility.writeTotalSizeOnHDFS;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.data.mdstore.manager.common.model.MDStoreVersion;
import eu.dnetlib.dhp.aggregation.common.AggregationCounter;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.model.mdstore.MetadataRecord;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class TransformSparkJobNode {

	private static final Logger log = LoggerFactory.getLogger(TransformSparkJobNode.class);

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
		// TODO this variable will be used after implementing Messaging with DNet Aggregator

		final ObjectMapper jsonMapper = new ObjectMapper();
		final MDStoreVersion nativeMdStoreVersion = jsonMapper.readValue(mdstoreInputVersion, MDStoreVersion.class);
		final MDStoreVersion cleanedMdStoreVersion = jsonMapper.readValue(mdstoreOutputVersion, MDStoreVersion.class);

		final String isLookupUrl = parser.get("isLookupUrl");
		log.info(String.format("isLookupUrl: %s", isLookupUrl));

		final String dateOfTransformation = parser.get("dateOfTransformation");
		log.info(String.format("dateOfTransformation: %s", dateOfTransformation));

		final ISLookUpService isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl);

		final VocabularyGroup vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService);

		log.info("Retrieved {} vocabularies", vocabularies.vocabularyNames().size());

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> transformRecords(
				parser.getObjectMap(), isLookupService, spark, nativeMdStoreVersion.getHdfsPath() + MDSTORE_DATA_PATH,
				cleanedMdStoreVersion.getHdfsPath() + MDSTORE_DATA_PATH));
	}

	public static void transformRecords(final Map<String, String> args, final ISLookUpService isLookUpService,
		final SparkSession spark, final String inputPath, final String outputPath)
		throws DnetTransformationException, IOException {

		final LongAccumulator totalItems = spark.sparkContext().longAccumulator(CONTENT_TOTALITEMS);
		final LongAccumulator errorItems = spark.sparkContext().longAccumulator(CONTENT_INVALIDRECORDS);
		final LongAccumulator transformedItems = spark.sparkContext().longAccumulator(CONTENT_TRANSFORMEDRECORDS);
		final AggregationCounter ct = new AggregationCounter(totalItems, errorItems, transformedItems);
		final Encoder<MetadataRecord> encoder = Encoders.bean(MetadataRecord.class);

		saveDataset(
			spark
				.read()
				.format("parquet")
				.load(inputPath)
				.as(encoder)
				.map(
					TransformationFactory.getTransformationPlugin(args, ct, isLookUpService),
					encoder),
			outputPath + MDSTORE_DATA_PATH);

		log.info("Transformed item " + ct.getProcessedItems().count());
		log.info("Total item " + ct.getTotalItems().count());
		log.info("Transformation Error item " + ct.getErrorItems().count());

		writeTotalSizeOnHDFS(spark, ct.getProcessedItems().count(), outputPath + MDSTORE_SIZE_PATH);
	}

}
