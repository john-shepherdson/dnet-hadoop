
package eu.dnetlib.dhp.actionmanager.bipaffiliations;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.Constants;
import eu.dnetlib.dhp.actionmanager.bipaffiliations.model.*;
import eu.dnetlib.dhp.actionmanager.ror.GenerateRorActionSetJob;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import scala.Tuple2;

/**
 * Creates action sets for Crossref affiliation relations inferred by BIP!
 */
public class PrepareAffiliationRelations implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(PrepareAffiliationRelations.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final String ID_PREFIX = "50|doi_________::";
	public static final String BIP_AFFILIATIONS_CLASSID = "result:organization:bipinference";
	public static final String BIP_AFFILIATIONS_CLASSNAME = "Affiliation relation inferred by BIP!";
	public static final String BIP_INFERENCE_PROVENANCE = "bip:affiliation:crossref";

	public static <I extends Result> void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareAffiliationRelations.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/bipaffiliations/input_actionset_parameter.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Constants.isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath {}: ", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Constants.removeOutputDir(spark, outputPath);
				prepareAffiliationRelations(spark, inputPath, outputPath);
			});
	}

	private static <I extends Result> void prepareAffiliationRelations(SparkSession spark, String inputPath,
		String outputPath) {

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		// load and parse affiliation relations from HDFS
		JavaRDD<AffiliationRelationDeserializer> affiliationRelationsDeserializeRDD = sc
			.textFile(inputPath)
			.map(item -> OBJECT_MAPPER.readValue(item, AffiliationRelationDeserializer.class));

		// convert affiliation to an internal representation
		Dataset<AffiliationRelationModel> affiliationRelations = spark
			.createDataset(
				affiliationRelationsDeserializeRDD
					.flatMap(
						entry -> entry
							.getMatchings()
							.stream()
							.flatMap(
								matching -> matching
									.getRorId()
									.stream()
									.map(
										rorId -> new AffiliationRelationModel(
											entry.getDoi(),
											rorId,
											matching.getConfidence())))
							.collect(Collectors.toList())
							.iterator())
					.rdd(),
				Encoders.bean(AffiliationRelationModel.class));

		// prepare action sets for affiliation relations
		affiliationRelations
			.flatMap((FlatMapFunction<AffiliationRelationModel, Relation>) affRel -> {

				// DOI to OpenAIRE id
				final String paperId = ID_PREFIX
					+ IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", affRel.getDoi()));

				// ROR id to OpenAIRE id
				final String affId = GenerateRorActionSetJob.calculateOpenaireId(affRel.getRorId());

				Qualifier qualifier = OafMapperUtils
					.qualifier(
						BIP_AFFILIATIONS_CLASSID,
						BIP_AFFILIATIONS_CLASSNAME,
						ModelConstants.DNET_PROVENANCE_ACTIONS,
						ModelConstants.DNET_PROVENANCE_ACTIONS);

				// format data info; setting `confidence` into relation's `trust`
				DataInfo dataInfo = OafMapperUtils
					.dataInfo(
						false,
						BIP_INFERENCE_PROVENANCE,
						true,
						false,
						qualifier,
						Double.toString(affRel.getConfidence()));

				// return bi-directional relations
				return getAffiliationRelationPair(paperId, affId, dataInfo).iterator();

			}, Encoders.bean(Relation.class))
			.toJavaRDD()
			.map(p -> new AtomicAction(Relation.class, p))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class);

	}

	private static List<Relation> getAffiliationRelationPair(String paperId, String affId, DataInfo dataInfo) {
		return Arrays
			.asList(
				OafMapperUtils
					.getRelation(
						paperId,
						affId,
						ModelConstants.RESULT_ORGANIZATION,
						ModelConstants.AFFILIATION,
						ModelConstants.HAS_AUTHOR_INSTITUTION,
						null,
						dataInfo,
						null),
				OafMapperUtils
					.getRelation(
						affId,
						paperId,
						ModelConstants.RESULT_ORGANIZATION,
						ModelConstants.AFFILIATION,
						ModelConstants.IS_AUTHOR_INSTITUTION_OF,
						null,
						dataInfo,
						null));
	}
}
