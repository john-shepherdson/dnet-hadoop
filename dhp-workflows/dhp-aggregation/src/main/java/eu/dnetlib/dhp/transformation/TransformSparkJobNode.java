
package eu.dnetlib.dhp.transformation;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.aggregation.common.AggregationCounter;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.model.mdstore.MetadataRecord;
import eu.dnetlib.dhp.transformation.vocabulary.VocabularyHelper;
import eu.dnetlib.dhp.transformation.xslt.XSLTTransformationFunction;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.message.Message;
import eu.dnetlib.message.MessageManager;
import eu.dnetlib.message.MessageType;

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

		final String inputPath = parser.get("mdstoreInputPath");
		final String outputPath = parser.get("mdstoreOutputPath");
		// TODO this variable will be used after implementing Messaging with DNet Aggregator

		final String isLookupUrl = parser.get("isLookupUrl");
		log.info(String.format("isLookupUrl: %s", isLookupUrl));

		final ISLookUpService isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> transformRecords(parser.getObjectMap(), isLookupService, spark, inputPath, outputPath));
	}

	public static void transformRecords(final Map<String, String> args, final ISLookUpService isLookUpService,
		final SparkSession spark, final String inputPath, final String outputPath) throws DnetTransformationException {

		final LongAccumulator totalItems = spark.sparkContext().longAccumulator("TotalItems");
		final LongAccumulator errorItems = spark.sparkContext().longAccumulator("errorItems");
		final LongAccumulator transformedItems = spark.sparkContext().longAccumulator("transformedItems");
		final AggregationCounter ct = new AggregationCounter(totalItems, errorItems, transformedItems);
		final Encoder<MetadataRecord> encoder = Encoders.bean(MetadataRecord.class);
		final Dataset<MetadataRecord> mdstoreInput = spark.read().format("parquet").load(inputPath).as(encoder);
		final MapFunction<MetadataRecord, MetadataRecord> XSLTTransformationFunction = TransformationFactory
			.getTransformationPlugin(args, ct, isLookUpService);
		mdstoreInput.map(XSLTTransformationFunction, encoder).write().save(outputPath);

		log.info("Transformed item " + ct.getProcessedItems().count());
		log.info("Total item " + ct.getTotalItems().count());
		log.info("Transformation Error item " + ct.getErrorItems().count());
	}

}
