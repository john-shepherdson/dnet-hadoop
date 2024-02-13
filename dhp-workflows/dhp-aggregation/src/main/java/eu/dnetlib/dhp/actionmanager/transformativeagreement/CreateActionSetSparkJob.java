
package eu.dnetlib.dhp.actionmanager.transformativeagreement;

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
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.transformativeagreement.model.TransformativeAgreementModel;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Country;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.utils.*;
import scala.Tuple2;

public class CreateActionSetSparkJob implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(CreateActionSetSparkJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final String IREL_PROJECT = "40|100018998___::1e5e62235d094afd01cd56e65112fc63";
	private static final String TRANSFORMATIVE_AGREEMENT = "openapc::transformativeagreement";

	public static void main(final String[] args) throws IOException, ParseException {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							CreateActionSetSparkJob.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/actionmanager/transformativeagreement/as_parameters.json"))));

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

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> createActionSet(spark, inputPath, outputPath));

	}

	private static void createActionSet(SparkSession spark, String inputPath, String outputPath) {
		spark
			.read()
			.textFile(inputPath)
			.map(
				(MapFunction<String, TransformativeAgreementModel>) value -> OBJECT_MAPPER
					.readValue(value, TransformativeAgreementModel.class),
				Encoders.bean(TransformativeAgreementModel.class))
			.flatMap(
				(FlatMapFunction<TransformativeAgreementModel, Relation>) value -> createRelation(
					value)
						.iterator(),
				Encoders.bean(Relation.class))
			.filter((FilterFunction<Relation>) Objects::nonNull)
			.toJavaRDD()
			.map(p -> new AtomicAction(p.getClass(), p))
			.union(
				spark
					.read()
					.textFile(inputPath)
					.map(
						(MapFunction<String, TransformativeAgreementModel>) value -> OBJECT_MAPPER
							.readValue(value, TransformativeAgreementModel.class),
						Encoders.bean(TransformativeAgreementModel.class))
					.map(
						(MapFunction<TransformativeAgreementModel, Result>) value -> createResult(
							value),
						Encoders.bean(Result.class))
					.filter((FilterFunction<Result>) r -> r != null)
					.toJavaRDD()
					.map(p -> new AtomicAction(p.getClass(), p)))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(
				outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);

	}

	private static Result createResult(TransformativeAgreementModel value) {
		Result r = new Result();
		r
			.setId(
				"50|doi_________::"
					+ IdentifierFactory
						.md5(PidCleaner.normalizePidValue(PidType.doi.toString(), value.getDoi())));
		r.setTransformativeAgreement(value.getAgreement());
		Country country = new Country();
		country.setClassid(value.getCountry());
		country.setClassname(value.getCountry());
		country
			.setDataInfo(
				OafMapperUtils
					.dataInfo(
						false, ModelConstants.SYSIMPORT_ACTIONSET, false, false,
						OafMapperUtils
							.qualifier(
								"openapc::transformativeagreement",
								"Harvested from Trnasformative Agreement file from OpenAPC",
								ModelConstants.DNET_PROVENANCE_ACTIONS, ModelConstants.DNET_PROVENANCE_ACTIONS),
						"0.9"));
		country.setSchemeid(ModelConstants.DNET_COUNTRY_TYPE);
		country.setSchemename(ModelConstants.DNET_COUNTRY_TYPE);
		r.setCountry(Arrays.asList(country));
		return r;
	}

	private static List<Relation> createRelation(TransformativeAgreementModel value) {

		List<Relation> relationList = new ArrayList<>();

		if (value.getAgreement().startsWith("IReL")) {
			String paper;

			paper = "50|doi_________::"
				+ IdentifierFactory
					.md5(PidCleaner.normalizePidValue(PidType.doi.toString(), value.getDoi()));

			relationList
				.add(
					getRelation(
						paper,
						IREL_PROJECT, ModelConstants.IS_PRODUCED_BY));

			relationList.add(getRelation(IREL_PROJECT, paper, ModelConstants.PRODUCES));
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
				ModelConstants.RESULT_PROJECT,
				ModelConstants.OUTCOME,
				relClass,
				Arrays
					.asList(
						OafMapperUtils.keyValue(ModelConstants.OPEN_APC_ID, ModelConstants.OPEN_APC_NAME)),
				OafMapperUtils
					.dataInfo(
						false, null, false, false,
						OafMapperUtils
							.qualifier(
								TRANSFORMATIVE_AGREEMENT, "Transformative Agreement",
								ModelConstants.DNET_PROVENANCE_ACTIONS, ModelConstants.DNET_PROVENANCE_ACTIONS),
						"0.9"),
				null);
	}

}
