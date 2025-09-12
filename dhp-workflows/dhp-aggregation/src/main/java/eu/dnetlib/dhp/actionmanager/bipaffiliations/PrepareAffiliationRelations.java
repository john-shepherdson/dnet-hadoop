
package eu.dnetlib.dhp.actionmanager.bipaffiliations;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.Constants;
import eu.dnetlib.dhp.actionmanager.ror.GenerateRorActionSetJob;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.DoiCleaningRule;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.utils.DHPUtils;
import scala.Tuple2;

/**
 * Creates action sets for Crossref affiliation relations inferred by OpenAIRE
 */
public class PrepareAffiliationRelations implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(PrepareAffiliationRelations.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final String ID_PREFIX = "50|doi_________::";
	public static final String BIP_AFFILIATIONS_CLASSID = "result:organization:openaireinference";
	public static final String BIP_AFFILIATIONS_CLASSNAME = "Affiliation relation inferred by OpenAIRE";
	public static final String BIP_INFERENCE_PROVENANCE = "openaire:affiliation";
	public static final String OPENAIRE_DATASOURCE_ID = "10|infrastruct_::f66f1bd369679b5b077dcdf006089556";
	public static final String OPENAIRE_DATASOURCE_NAME = "OpenAIRE";
	public static final String DOI_URL_PREFIX = "https://doi.org/";
	public static final int DOI_URL_PREFIX_LENGTH = 16;
	private static final String OPENORGS_NS_PREFIX = "openorgs____";

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

		final String crossrefInputPath = parser.get("crossrefInputPath");
		log.info("crossrefInputPath: {}", crossrefInputPath);

		final String pubmedInputPath = parser.get("pubmedInputPath");
		log.info("pubmedInputPath: {}", pubmedInputPath);

		final String openapcInputPath = parser.get("openapcInputPath");
		log.info("openapcInputPath: {}", openapcInputPath);

		final String dataciteInputPath = parser.get("dataciteInputPath");
		log.info("dataciteInputPath: {}", dataciteInputPath);

		final String inputPaths = parser.get("inputPaths");
		log.info("inputPaths: {}", inputPaths);

//		final String webcrawlInputPath = parser.get("webCrawlInputPath");
//		log.info("webcrawlInputPath: {}", webcrawlInputPath);
//
//		final String publisherInputPath = parser.get("publisherInputPath");
//		log.info("publisherInputPath: {}", publisherInputPath);
//
//		final String graphInputPath = parser.get("graphInputPath");
//		log.info("graphInputPath: {}", graphInputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Constants.removeOutputDir(spark, outputPath);
				createActionSet(
					spark, crossrefInputPath, pubmedInputPath, openapcInputPath, dataciteInputPath, inputPaths,
					 outputPath);
			});
	}

	private static void createActionSet(SparkSession spark, String crossrefInputPath, String pubmedInputPath,
		String openapcInputPath, String dataciteInputPath, String inputPaths, String outputPath) {
		List<KeyValue> collectedfromOpenAIRE = OafMapperUtils
			.listKeyValues(OPENAIRE_DATASOURCE_ID, OPENAIRE_DATASOURCE_NAME);

		JavaPairRDD<Text, Text> crossrefRelations = prepareAffiliationRelationsCrossref(
			spark, crossrefInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":crossref");

		JavaPairRDD<Text, Text> pubmedRelations = prepareAffiliationRelationFromPublisherOldModel(
			spark, pubmedInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":pubmed");

		JavaPairRDD<Text, Text> openAPCRelations = prepareAffiliationRelationsOpenAPC(
			spark, openapcInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":openapc");

		JavaPairRDD<Text, Text> dataciteRelations = prepareAffiliationRelationFromPublisherOldModel(
			spark, dataciteInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":datacite");


		JavaPairRDD<Text, Text> oalexRelations = prepareAffiliationRelationsGraph(
				spark, inputPaths + "/oalex", collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":rawaff");

		JavaPairRDD<Text, Text> publisherRelations = prepareAffiliationRelationsGraph(
				spark, inputPaths + "/publishers", collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":webcrawl");

		JavaPairRDD<Text, Text> oaireRelations = prepareAffiliationRelationsGraph(
				spark, inputPaths + "/oaire", collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":graph");



		crossrefRelations
			.union(pubmedRelations)
			.union(openAPCRelations)
			.union(dataciteRelations)
			.union(oalexRelations)
			.union(publisherRelations)
			.union(oaireRelations)
			.saveAsHadoopFile(
				outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, BZip2Codec.class);
	}

	private static JavaPairRDD<Text, Text> prepareAffiliationRelationsGraph(SparkSession spark, String datasetPath, List<KeyValue> collectedfromOpenAIRE, String dataprovenance) {
		Dataset<Row> df = spark.read().schema(
						"`id` STRING, `organizations` ARRAY<STRUCT<`pid`:STRING, `value`:STRING, `name` :STRING, `confidence`:DOUBLE, `status`:STRING, `country` :STRING>>").json(datasetPath)
				.select("id","organizations")
				.withColumn("matching", functions.explode(new Column("organizations")))
				.select(new Column("id").as("id"),
						new Column("matching.pid").as("pidtype"),
						new Column("matching.value").as("pidvalue"),
						new Column("matching.confidence").as("confidence"),
						new Column("matching.status").as("status"))
				.where("status = 'active'");

		return getTextTextJavaPairRDDNew(
				collectedfromOpenAIRE, df.selectExpr("id", "matching"), dataprovenance, false);
	}


	private static JavaPairRDD<Text, Text> prepareAffiliationRelationFromPublisherOldModel(SparkSession spark,
																						   String inputPath,
																						   List<KeyValue> collectedfrom,
																						   String dataprovenance) {

		Dataset<Row> df = spark
				.read()
				.schema(
						"`DOI` STRING, `Organizations` ARRAY<STRUCT<`PID`:STRING, `Value`:STRING,`Confidence`:DOUBLE, `Status`:STRING>>")
				.json(inputPath)
				.where("DOI is not null");

		return getTextTextJavaPairRDDOld(
				collectedfrom, df.selectExpr("DOI", "Organizations as Matchings"), dataprovenance, true);

	}

	private static JavaPairRDD<Text, Text> prepareAffiliationRelationFromPublisherNewModel(SparkSession spark,
		String inputPath,
		List<KeyValue> collectedfrom,
		String dataprovenance) {

		Dataset<Row> df = spark
			.read()
			.schema(
				"`doi` STRING, `matchings` ARRAY<STRUCT<`pid`:STRING, `value`:STRING, `name` :STRING, `confidence`:DOUBLE, `status`:STRING, `country` :STRING>>")
			.json(inputPath)
			.where("doi is not null");

		return getTextTextJavaPairRDDNew(
			collectedfrom, df.selectExpr("doi", "matchings"), dataprovenance, true);

	}

	private static <I extends Result> JavaPairRDD<Text, Text> prepareAffiliationRelationsCrossref(SparkSession spark,
																								  String inputPath,
																								  List<KeyValue> collectedfrom, String dataprovenance) {
		// load and parse affiliation relations from HDFS
		Dataset<Row> df = spark
			.read()
			.schema(
				"`DOI` STRING, `Matchings` ARRAY<STRUCT<`PID`:STRING, `Value`:STRING,`Confidence`:DOUBLE, `Status`:STRING>>")
			.json(inputPath)
			.where("DOI is not null");

		return getTextTextJavaPairRDDOld(collectedfrom, df, dataprovenance, true);
	}

	private static <I extends Result> JavaPairRDD<Text, Text> prepareAffiliationRelationsOpenAPC(SparkSession spark,
																								  String inputPath,
																								  List<KeyValue> collectedfrom, String dataprovenance) {
		// load and parse affiliation relations from HDFS
		Dataset<Row> df = spark
				.read()
				.schema(
						"`doi` STRING, `matchings` ARRAY<STRUCT<`pid`:STRING, `value`:STRING,`confidence`:DOUBLE, `status`:STRING>>")
				.json(inputPath)
				.where("doi is not null");

		return getTextTextJavaPairRDDNew(collectedfrom, df, dataprovenance, true);
	}

	private static JavaPairRDD<Text, Text> getTextTextJavaPairRDDOld(List<KeyValue> collectedfrom, Dataset<Row> df,
																	 String dataprovenance, boolean isDoi) {
		// unroll nested arrays
		if (isDoi)
			df = df
					.withColumn("matching", functions.explode(new Column("Matchings")))
					.select(
							new Column("DOI").as("id"),
							new Column("matching.PID").as("pidtype"),
							new Column("matching.Value").as("pidvalue"),
							new Column("matching.Confidence").as("confidence"),
							new Column("matching.Status").as("status"))
					.where(functions.col("status").equalTo("active"))
					.where(functions.col("pidvalue").notEqual(""));
		else
			df = df
					.withColumn("matching", functions.explode(new Column("Matchings")))
					.select(
							new Column("id").as("id"),
							new Column("matching.PID").as("pidtype"),
							new Column("matching.Value").as("pidvalue"),
							new Column("matching.Confidence").as("confidence"),
							new Column("matching.Status").as("status"))
					.where(functions.col("status").equalTo("active"))
					.where(functions.col("pidvalue").notEqual(""));

		// prepare action sets for affiliation relations
		return df
				.toJavaRDD()
				.flatMap((FlatMapFunction<Row, Relation>) row -> {

					// DOI to OpenAIRE id
					String resultId = row.getAs("id");
					if (isDoi)
						resultId = ID_PREFIX
								+ IdentifierFactory.md5(DoiCleaningRule.clean(removePrefix(resultId)));

					// Organization to OpenAIRE identifier
					String affId = null;
					if (row.getAs("pidtype").equals("ROR"))
						// ROR id to OpenIARE id
						affId = GenerateRorActionSetJob.calculateOpenaireId(row.getAs("pidvalue"));
					else
						// getting the OpenOrgs identifier for the organization
						affId = calculateOpenOrgsId(row.getAs("pidvalue"));

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
									dataprovenance,
									true,
									false,
									qualifier,
									Double.toString(row.getAs("confidence")));

					// return bi-directional relations
					return getAffiliationRelationPair(resultId, affId, collectedfrom, dataInfo).iterator();

				})
				.map(p -> new AtomicAction(Relation.class, p))
				.mapToPair(
						aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
								new Text(OBJECT_MAPPER.writeValueAsString(aa))));
	}
	private static JavaPairRDD<Text, Text> getTextTextJavaPairRDDNew(List<KeyValue> collectedfrom, Dataset<Row> df,
		String dataprovenance, boolean isDoi) {
		// unroll nested arrays
		if (isDoi)
			df = df

				.select(
					new Column("doi").as("id"),
					new Column("matching.pid").as("pidtype"),
					new Column("matching.value").as("pidvalue"),
					new Column("matching.confidence").as("confidence"),
					new Column("matching.status").as("status"))
				.where(functions.col("status").equalTo("active"))
				.where(functions.col("pidvalue").notEqual(""));
		else
			df = df

				.select(
					new Column("id").as("id"),
					new Column("matching.pid").as("pidtype"),
					new Column("matching.value").as("pidvalue"),
					new Column("matching.confidence").as("confidence"),
					new Column("matching.status").as("status"))
				.where(functions.col("status").equalTo("active"))
				.where(functions.col("pidvalue").notEqual(""));

		// prepare action sets for affiliation relations
		return df
			.toJavaRDD()
			.flatMap((FlatMapFunction<Row, Relation>) row -> {

				// DOI to OpenAIRE id
				String resultId = row.getAs("id");
				if (isDoi)
					resultId = ID_PREFIX
						+ IdentifierFactory.md5(DoiCleaningRule.clean(removePrefix(resultId)));

				// Organization to OpenAIRE identifier
				String affId = null;
				if ("ROR".equalsIgnoreCase(row.getAs("pidtype")))
					// ROR id to OpenIARE id
					affId = GenerateRorActionSetJob.calculateOpenaireId(row.getAs("pidvalue"));
				else
					// getting the OpenOrgs identifier for the organization
					affId = calculateOpenOrgsId(row.getAs("pidvalue"));

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
						dataprovenance,
						true,
						false,
						qualifier,
						Double.toString(row.getAs("confidence")));

				// return bi-directional relations
				return getAffiliationRelationPair(resultId, affId, collectedfrom, dataInfo).iterator();

			})
			.map(p -> new AtomicAction(Relation.class, p))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))));
	}

	private static String calculateOpenOrgsId(String pidvalue) {
		if (pidvalue.contains(OPENORGS_NS_PREFIX)) {
			pidvalue = StringUtils.substringAfter(pidvalue, "::");
		}

		return String.format("20|%s::%s", OPENORGS_NS_PREFIX, DHPUtils.md5(pidvalue));

	}

	private static String removePrefix(String doi) {
		if (doi.startsWith(DOI_URL_PREFIX))
			return doi.substring(DOI_URL_PREFIX_LENGTH);
		return doi;
	}

	private static List<Relation> getAffiliationRelationPair(String paperId, String affId, List<KeyValue> collectedfrom,
		DataInfo dataInfo) {
		return Arrays
			.asList(
				OafMapperUtils
					.getRelation(
						paperId,
						affId,
						ModelConstants.RESULT_ORGANIZATION,
						ModelConstants.AFFILIATION,
						ModelConstants.HAS_AUTHOR_INSTITUTION,
						collectedfrom,
						dataInfo,
						null),
				OafMapperUtils
					.getRelation(
						affId,
						paperId,
						ModelConstants.RESULT_ORGANIZATION,
						ModelConstants.AFFILIATION,
						ModelConstants.IS_AUTHOR_INSTITUTION_OF,
						collectedfrom,
						dataInfo,
						null));
	}
}
