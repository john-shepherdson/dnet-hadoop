
package eu.dnetlib.dhp.actionmanager.bipaffiliations;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
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
import eu.dnetlib.dhp.schema.oaf.utils.CleaningFunctions;
import eu.dnetlib.dhp.schema.oaf.utils.DoiCleaningRule;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
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

		final String webcrawlInputPath = parser.get("webCrawlInputPath");
		log.info("webcrawlInputPath: {}", webcrawlInputPath);

		final String publisherInputPath = parser.get("publisherInputPath");
		log.info("publisherInputPath: {}", publisherInputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Constants.removeOutputDir(spark, outputPath);
				createActionSet(
					spark, crossrefInputPath, pubmedInputPath, openapcInputPath, dataciteInputPath, webcrawlInputPath,
					publisherInputPath, outputPath);
			});
	}

	private static void createActionSet(SparkSession spark, String crossrefInputPath, String pubmedInputPath,
		String openapcInputPath, String dataciteInputPath, String webcrawlInputPath, String publisherlInputPath,
		String outputPath) {
		List<KeyValue> collectedfromOpenAIRE = OafMapperUtils
			.listKeyValues(OPENAIRE_DATASOURCE_ID, OPENAIRE_DATASOURCE_NAME);

		JavaPairRDD<Text, Text> crossrefRelations = prepareAffiliationRelationsNewModel(
			spark, crossrefInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":crossref");

		JavaPairRDD<Text, Text> pubmedRelations = prepareAffiliationRelations(
			spark, pubmedInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":pubmed");

		JavaPairRDD<Text, Text> openAPCRelations = prepareAffiliationRelationsNewModel(
			spark, openapcInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":openapc");

		JavaPairRDD<Text, Text> dataciteRelations = prepareAffiliationRelationsNewModel(
			spark, dataciteInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":datacite");

		JavaPairRDD<Text, Text> webCrawlRelations = prepareAffiliationRelationsNewModel(
			spark, webcrawlInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":rawaff");

		JavaPairRDD<Text, Text> publisherRelations = prepareAffiliationRelationFromPublisherNewModel(
			spark, publisherlInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE + ":webcrawl");

		crossrefRelations
			.union(pubmedRelations)
			.union(openAPCRelations)
			.union(dataciteRelations)
			.union(webCrawlRelations)
			.union(publisherRelations)
			.saveAsHadoopFile(
				outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, BZip2Codec.class);
	}

	private static JavaPairRDD<Text, Text> prepareAffiliationRelationFromPublisherNewModel(SparkSession spark,
		String inputPath,
		List<KeyValue> collectedfrom,
		String dataprovenance) {

		Dataset<Row> df = spark
			.read()
			.schema(
				"`DOI` STRING, `Organizations` ARRAY<STRUCT<`PID`:STRING, `Value`:STRING,`Confidence`:DOUBLE, `Status`:STRING>>")
			.json(inputPath)
			.where("DOI is not null");

		return getTextTextJavaPairRDDNew(
			collectedfrom, df.selectExpr("DOI", "Organizations as Matchings"), dataprovenance);

	}

	private static JavaPairRDD<Text, Text> prepareAffiliationRelationFromPublisher(SparkSession spark, String inputPath,
		List<KeyValue> collectedfrom, String dataprovenance) {

		Dataset<Row> df = spark
			.read()
			.schema("`DOI` STRING, `Organizations` ARRAY<STRUCT<`RORid`:STRING,`Confidence`:DOUBLE>>")
			.json(inputPath)
			.where("DOI is not null");

		return getTextTextJavaPairRDD(
			collectedfrom, df.selectExpr("DOI", "Organizations as Matchings"), dataprovenance);

	}

	private static <I extends Result> JavaPairRDD<Text, Text> prepareAffiliationRelations(SparkSession spark,
		String inputPath,
		List<KeyValue> collectedfrom, String dataprovenance) {

		// load and parse affiliation relations from HDFS
		Dataset<Row> df = spark
			.read()
			.schema("`DOI` STRING, `Matchings` ARRAY<STRUCT<`RORid`:STRING,`Confidence`:DOUBLE>>")
			.json(inputPath)
			.where("DOI is not null");

		return getTextTextJavaPairRDD(collectedfrom, df, dataprovenance);
	}

	private static <I extends Result> JavaPairRDD<Text, Text> prepareAffiliationRelationsNewModel(SparkSession spark,
		String inputPath,
		List<KeyValue> collectedfrom, String dataprovenance) {
		// load and parse affiliation relations from HDFS
		Dataset<Row> df = spark
			.read()
			.schema(
				"`DOI` STRING, `Matchings` ARRAY<STRUCT<`PID`:STRING, `Value`:STRING,`Confidence`:DOUBLE, `Status`:STRING>>")
			.json(inputPath)
			.where("DOI is not null");

		return getTextTextJavaPairRDDNew(collectedfrom, df, dataprovenance);
	}

	private static JavaPairRDD<Text, Text> getTextTextJavaPairRDD(List<KeyValue> collectedfrom, Dataset<Row> df,
		String dataprovenance) {
		// unroll nested arrays
		df = df
			.withColumn("matching", functions.explode(new Column("Matchings")))
			.select(
				new Column("DOI").as("doi"),
				new Column("matching.RORid").as("rorid"),
				new Column("matching.Confidence").as("confidence"));

		// prepare action sets for affiliation relations
		return df
			.toJavaRDD()
			.flatMap((FlatMapFunction<Row, Relation>) row -> {

				// DOI to OpenAIRE id
				final String paperId = ID_PREFIX
					+ IdentifierFactory.md5(DoiCleaningRule.clean(removePrefix(row.getAs("doi"))));

				// ROR id to OpenAIRE id
				final String affId = GenerateRorActionSetJob.calculateOpenaireId(row.getAs("rorid"));

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

	private static JavaPairRDD<Text, Text> getTextTextJavaPairRDDNew(List<KeyValue> collectedfrom, Dataset<Row> df,
		String dataprovenance) {
		// unroll nested arrays
		df = df
			.withColumn("matching", functions.explode(new Column("Matchings")))
			.select(
				new Column("DOI").as("doi"),
				new Column("matching.PID").as("pidtype"),
				new Column("matching.Value").as("pidvalue"),
				new Column("matching.Confidence").as("confidence"),
				new Column("matching.Status").as("status"))
			.where("status = 'active'");

		// prepare action sets for affiliation relations
		return df
			.toJavaRDD()
			.flatMap((FlatMapFunction<Row, Relation>) row -> {

				// DOI to OpenAIRE id
				final String paperId = ID_PREFIX
					+ IdentifierFactory.md5(DoiCleaningRule.clean(removePrefix(row.getAs("doi"))));

				// Organization to OpenAIRE identifier
				String affId = null;
				if (row.getAs("pidtype").equals("ROR"))
					// ROR id to OpenIARE id
					affId = GenerateRorActionSetJob.calculateOpenaireId(row.getAs("pidvalue"));
				else
					// getting the OpenOrgs identifier for the organization
					affId = row.getAs("pidvalue");

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
