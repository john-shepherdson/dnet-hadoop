
package eu.dnetlib.dhp.actionmanager.opencitations;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.opencitations.model.COCI;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.*;
import eu.dnetlib.dhp.utils.DHPUtils;
import scala.Tuple2;

public class CreateActionSetSparkJob implements Serializable {
	public static final String OPENCITATIONS_CLASSID = "sysimport:crosswalk:opencitations";
	public static final String OPENCITATIONS_CLASSNAME = "Imported from OpenCitations";

	private static final String DOI_PREFIX = "50|doi_________::";

	private static final String PMID_PREFIX = "50|pmid________::";

	private static final String TRUST = "0.91";

	private static final Logger log = LoggerFactory.getLogger(CreateActionSetSparkJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws IOException, ParseException {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							CreateActionSetSparkJob.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/actionmanager/opencitations/as_parameters.json"))));

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}", outputPath);

		final boolean shouldDuplicateRels = Optional
			.ofNullable(parser.get("shouldDuplicateRels"))
			.map(Boolean::valueOf)
			.orElse(Boolean.FALSE);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> extractContent(spark, inputPath, outputPath, shouldDuplicateRels));

	}

	private static void extractContent(SparkSession spark, String inputPath, String outputPath,
		boolean shouldDuplicateRels) {

		getTextTextJavaPairRDD(spark, inputPath)
			// .union(getTextTextJavaPairRDD(spark, inputPath, shouldDuplicateRels, POCI))
			.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);
	}

	private static JavaPairRDD<Text, Text> getTextTextJavaPairRDD(SparkSession spark, String inputPath) {
		return spark
			.read()
			.textFile(inputPath)
			.map(
				(MapFunction<String, COCI>) value -> OBJECT_MAPPER.readValue(value, COCI.class),
				Encoders.bean(COCI.class))
			.flatMap(
				(FlatMapFunction<COCI, Relation>) value -> createRelation(
					value)
						.iterator(),
				Encoders.bean(Relation.class))
			.filter((FilterFunction<Relation>) Objects::nonNull)
			.toJavaRDD()
			.map(p -> new AtomicAction(p.getClass(), p))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))));
	}

	private static List<Relation> createRelation(COCI value) {

		List<Relation> relationList = new ArrayList<>();

		String citing;
		String cited;

		switch (value.getCiting_pid()) {
			case "doi":
				citing = DOI_PREFIX
					+ IdentifierFactory
						.md5(PidCleaner.normalizePidValue(PidType.doi.toString(), value.getCiting()));
				break;
			case "pmid":
				citing = PMID_PREFIX
					+ IdentifierFactory
						.md5(PidCleaner.normalizePidValue(PidType.doi.toString(), value.getCiting()));
				break;
			default:
				throw new IllegalStateException("Invalid prefix: " + value.getCiting_pid());
		}

		switch (value.getCited_pid()) {
			case "doi":
				cited = DOI_PREFIX
					+ IdentifierFactory
						.md5(PidCleaner.normalizePidValue(PidType.doi.toString(), value.getCiting()));
				break;
			case "pmid":
				cited = PMID_PREFIX
					+ IdentifierFactory
						.md5(PidCleaner.normalizePidValue(PidType.doi.toString(), value.getCiting()));
				break;
			default:
				throw new IllegalStateException("Invalid prefix: " + value.getCited_pid());
		}

		if (!citing.equals(cited)) {
			relationList
				.add(
					getRelation(
						citing,
						cited, ModelConstants.CITES));
		}

		return relationList;
	}

	public static Relation getRelation(
		String source,
		String target,
		String relClass) {

		return OafMapperUtils
			.getRelation(
				source,
				target,
				ModelConstants.RESULT_RESULT,
				ModelConstants.CITATION,
				relClass,
				Arrays
					.asList(
						OafMapperUtils.keyValue(ModelConstants.OPENOCITATIONS_ID, ModelConstants.OPENOCITATIONS_NAME)),
				OafMapperUtils
					.dataInfo(
						false, null, false, false,
						OafMapperUtils
							.qualifier(
								OPENCITATIONS_CLASSID, OPENCITATIONS_CLASSNAME,
								ModelConstants.DNET_PROVENANCE_ACTIONS, ModelConstants.DNET_PROVENANCE_ACTIONS),
						TRUST),
				null);
	}

}
