
package eu.dnetlib.dhp.actionmanager.affro;

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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.functions.expr;

/**
 * Creates action sets for Crossref affiliation relations inferred by OpenAIRE
 */
public class PrepareAffiliationRelations2 implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(PrepareAffiliationRelations2.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final String ID_PREFIX = "50|doi_________::";
	public static final String BIP_AFFILIATIONS_CLASSID = "result:organization:openaireinference";
	public static final String BIP_AFFILIATIONS_CLASSNAME = "Affiliation relation inferred by OpenAIRE";
	public static final String BIP_INFERENCE_PROVENANCE = "openaire:affiliation";
	public static final String OPENAIRE_DATASOURCE_ID = "10|infrastruct_::f66f1bd369679b5b077dcdf006089556";
	public static final String OPENAIRE_DATASOURCE_NAME = "OpenAIRE";
	public static final String DOI_URL_PREFIX = "https://doi.org/";
	public static final int DOI_URL_PREFIX_LENGTH = 16;
	private static final Object OPENORGS_NS_PREFIX = "openorgs____";

	public static <I extends Result> void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareAffiliationRelations2.class
					.getResourceAsStream(
							"/eu/dnetlib/dhp/actionmanager/affro/input_actionset_parameter.json"));

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

		final String inputPaths = parser.get("inputPaths");// oalex
		log.info("inputPaths: {}", inputPaths);


//		final String oalexInputPath = parser.get("oalexInputPath");// oalex
//		log.info("oalexInputPath: {}", oalexInputPath);
//
//		final String publisherInputPath = parser.get("publisherInputPath");
//		log.info("publisherInputPath: {}", publisherInputPath);
//
//		final String oaireInputPath = parser.get("oaireInputPath");
//		log.info("oaireInputPath: {}", oaireInputPath);
//
//		final String iisInputPath = parser.get("iisInputPath");
//		log.info("iisInputPath: {}", iisInputPath);

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

		JavaPairRDD<Text, Text> crossrefRelations = prepareAffiliationRelations(
			spark, crossrefInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":crossref");

		JavaPairRDD<Text, Text> pubmedRelations = prepareAffiliationRelations(
			spark, pubmedInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":pubmed");

		JavaPairRDD<Text, Text> openAPCRelations = prepareAffiliationRelations(
			spark, openapcInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":openapc");

		JavaPairRDD<Text, Text> dataciteRelations = prepareAffiliationRelations(
			spark, dataciteInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":datacite");

		JavaPairRDD<Text, Text> oalexRelations = prepareAffiliationRelations(
			spark, inputPaths + "/oalex", collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":rawaff");

		JavaPairRDD<Text, Text> publisherRelations = prepareAffiliationRelations(
			spark, inputPaths + "/publishers", collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":webcrawl");

		JavaPairRDD<Text, Text> oaireRelations = prepareAffiliationRelations(
				spark, inputPaths + "/oaire", collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":graph");

		JavaPairRDD<Text, Text> iisRelations = prepareAffiliationRelations(
				spark, inputPaths + "/iis", collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":iis");

		crossrefRelations
			.union(pubmedRelations)
			.union(openAPCRelations)
			.union(dataciteRelations)
			.union(oalexRelations)
			.union(publisherRelations)
				.union(oaireRelations)
				.union(iisRelations)
			.saveAsHadoopFile(
				outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, BZip2Codec.class);
	}

//	private static <I extends Result> JavaPairRDD<Text, Text> prepareAffiliationRelations(SparkSession spark,
//																						  String inputPath,
//																						  List<KeyValue> collectedfrom, String dataprovenance) {
//
//		spark
//				.udf()
//				.register(
//						"md5HashWithPrefix", (String doi) -> ID_PREFIX + IdentifierFactory.md5(DoiCleaningRule.clean(removePrefix(doi))), DataTypes.StringType);
//
//		Dataset<Row> df = spark
//				.read()
//				.schema("`DOI` STRING, `Matchings` ARRAY<STRUCT<`RORid`:STRING,`Confidence`:DOUBLE>>")
//				.json(inputPath)
//				.where("DOI is not null")
//				.withColumn("id",  expr("md5HashWithPrefix(doi)"))
//				.withColumn("matching",functions.explode(new Column("Matchings")) )
//				.select("id", "matching");;
//
//		return getTextTextJavaPairRDD(collectedfrom, df, dataprovenance);
//	}
//
//	private static JavaPairRDD<Text, Text> getTextTextJavaPairRDD(List<KeyValue> collectedfrom, Dataset<Row> df,
//																  String dataprovenance) {
//		// unroll nested arrays
//		df = df
//				.select(
//						new Column("id").as("id"),
//						new Column("matching.RORid").as("rorid"),
//						new Column("matching.Confidence").as("confidence"));
//
//		// prepare action sets for affiliation relations
//		return df
//				.toJavaRDD()
//				.flatMap((FlatMapFunction<Row, Relation>) row -> {
//
//					// DOI to OpenAIRE id
//					final String paperId = ID_PREFIX
//							+ IdentifierFactory.md5(DoiCleaningRule.clean(removePrefix(row.getAs("doi"))));
//
//					// ROR id to OpenAIRE id
//					final String affId = GenerateRorActionSetJob.calculateOpenaireId(row.getAs("rorid"));
//
//					Qualifier qualifier = OafMapperUtils
//							.qualifier(
//									BIP_AFFILIATIONS_CLASSID,
//									BIP_AFFILIATIONS_CLASSNAME,
//									ModelConstants.DNET_PROVENANCE_ACTIONS,
//									ModelConstants.DNET_PROVENANCE_ACTIONS);
//
//					// format data info; setting `confidence` into relation's `trust`
//					DataInfo dataInfo = OafMapperUtils
//							.dataInfo(
//									false,
//									dataprovenance,
//									true,
//									false,
//									qualifier,
//									Double.toString(row.getAs("confidence")));
//
//					// return bi-directional relations
//					return getAffiliationRelationPair(paperId, affId, collectedfrom, dataInfo).iterator();
//
//				})
//				.map(p -> new AtomicAction(Relation.class, p))
//				.mapToPair(
//						aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
//								new Text(OBJECT_MAPPER.writeValueAsString(aa))));
//	}
//
//	private static JavaPairRDD<Text, Text> prepareAffiliationRelationsGraph(SparkSession spark, String datasetPath, List<KeyValue> collectedfromOpenAIRE, String dataprovenance) {
//		Dataset<Row> df = spark.read().schema(eu.dnetlib.dhp.actionmanager.affro.Constants.RESULT_MATCHED_SCHEMA).json(datasetPath)
//				.select("id","organizations")
//				.withColumn("matching", functions.explode(new Column("organizations")))
//				.select(new Column("id").as("id"),
//						new Column("matching.pid").as("pidtype"),
//						new Column("matching.value").as("pidvalue"),
//						new Column("matching.confidence").as("confidence"),
//						new Column("matching.status").as("status"),
//						new Column("matching.name").as("name"),
//						new Column("matching.country").as("country"))
//				.where("status = 'active'");
//
//		return getTextTextJavaPairRDDNew(
//				collectedfromOpenAIRE, df.selectExpr("id", "matching"), dataprovenance);
//	}
//
//	private static JavaPairRDD<Text, Text> prepareAffiliationRelationFromPublisherNewModel(SparkSession spark,
//		String inputPath,
//		List<KeyValue> collectedfrom,
//		String dataprovenance) {
//
//		spark
//				.udf()
//				.register(
//						"md5HashWithPrefix", (String doi) -> ID_PREFIX + IdentifierFactory.md5(DoiCleaningRule.clean(removePrefix(doi))), DataTypes.StringType);
//
//		Dataset<Row> df = spark
//			.read()
//			.schema(
//				"`DOI` STRING, `Organizations` ARRAY<STRUCT<`PID`:STRING, `Value`:STRING,`Confidence`:DOUBLE, `Status`:STRING>>")
//			.json(inputPath)
//			.where("DOI is not null")
//				.withColumn("id",  expr("md5HashWithPrefix(doi)"))
//				.withColumn("matching",functions.explode(new Column("Matchings")) )
//				.select("id", "matching");;
//
//		return getTextTextJavaPairRDDNew(
//			collectedfrom, df.selectExpr("id", "matching"), dataprovenance);
//
//	}


	private static <I extends Result> JavaPairRDD<Text, Text> prepareAffiliationRelations(SparkSession spark,
		String inputPath,
		List<KeyValue> collectedfrom, String dataprovenance) {

		spark
				.udf()
				.register(
						"md5HashWithPrefix", (String doi) -> ID_PREFIX + IdentifierFactory.md5(DoiCleaningRule.clean(removePrefix(doi))), DataTypes.StringType);
		// load and parse affiliation relations from HDFS
		Dataset<Row> df = spark
			.read()
			.schema(
				"`id` STRING, `organizations` ARRAY<STRUCT<`pid`:STRING, `value`:STRING,`confidence`:DOUBLE, `status`:STRING, `country`: STRING, `name`: STRING>>")
			.json(inputPath)
			.where("id is not null")
				.withColumn("id",  expr("md5HashWithPrefix(doi)"))
				.withColumn("matching",functions.explode(new Column("matchings")) )
				.select("id", "matching");

		return getTextTextJavaPairRDDNew(collectedfrom, df, dataprovenance);
	}

	private static JavaPairRDD<Text, Text> getTextTextJavaPairRDDNew(List<KeyValue> collectedfrom, Dataset<Row> df,
		String dataprovenance) {


		// unroll nested arrays
		df = df
				.select(
				new Column("id").as("id"),
				new Column("matching.pid").as("pidtype"),
				new Column("matching.value").as("pidvalue"),
				new Column("matching.donfidence").as("confidence"),
				new Column("matching.status").as("status"),
						new Column("matching.country").as("country"),
						new Column("matching.name").as("name"))
			.where("status = 'active'");

		// prepare action sets for affiliation relations
		return df
			.toJavaRDD()
			.flatMap((FlatMapFunction<Row, Relation>) row -> {

				// DOI to OpenAIRE id
				final String paperId = row.getAs("id");

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
				return getAffiliationRelationPair(paperId, affId, collectedfrom, dataInfo).iterator();

			})
			.map(p -> new AtomicAction(Relation.class, p))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))));
	}

	private static String calculateOpenOrgsId(String pidvalue) {

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
