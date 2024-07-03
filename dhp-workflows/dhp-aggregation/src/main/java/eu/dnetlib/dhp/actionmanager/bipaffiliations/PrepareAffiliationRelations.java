
package eu.dnetlib.dhp.actionmanager.bipaffiliations;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
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
	public static final String BIP_AFFILIATIONS_CLASSID = "result:organization:openaireinference";
	public static final String BIP_AFFILIATIONS_CLASSNAME = "Affiliation relation inferred by OpenAIRE";
	public static final String BIP_INFERENCE_PROVENANCE = "openaire:affiliation";

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

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Constants.removeOutputDir(spark, outputPath);

				List<KeyValue> collectedFromCrossref = OafMapperUtils
					.listKeyValues(ModelConstants.CROSSREF_ID, "Crossref");
				JavaPairRDD<Text, Text> crossrefRelations = prepareAffiliationRelations(
					spark, crossrefInputPath, collectedFromCrossref);

				List<KeyValue> collectedFromPubmed = OafMapperUtils
					.listKeyValues(ModelConstants.PUBMED_CENTRAL_ID, "Pubmed");
				JavaPairRDD<Text, Text> pubmedRelations = prepareAffiliationRelations(
					spark, pubmedInputPath, collectedFromPubmed);

				List<KeyValue> collectedFromOpenAPC = OafMapperUtils
					.listKeyValues(ModelConstants.OPEN_APC_ID, "OpenAPC");
				JavaPairRDD<Text, Text> openAPCRelations = prepareAffiliationRelations(
					spark, openapcInputPath, collectedFromOpenAPC);

				List<KeyValue> collectedFromDatacite = OafMapperUtils
					.listKeyValues(ModelConstants.DATACITE_ID, "Datacite");
				JavaPairRDD<Text, Text> dataciteRelations = prepareAffiliationRelations(
					spark, dataciteInputPath, collectedFromDatacite);

				List<KeyValue> collectedFromWebCrawl = OafMapperUtils
					.listKeyValues(Constants.WEB_CRAWL_ID, Constants.WEB_CRAWL_NAME);
				JavaPairRDD<Text, Text> webCrawlRelations = prepareAffiliationRelations(
					spark, webcrawlInputPath, collectedFromWebCrawl);

				crossrefRelations
					.union(pubmedRelations)
					.union(openAPCRelations)
					.union(dataciteRelations)
					.union(webCrawlRelations)
					.saveAsHadoopFile(
						outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, BZip2Codec.class);

			});
	}

	private static <I extends Result> JavaPairRDD<Text, Text> prepareAffiliationRelations(SparkSession spark,
		String inputPath,
		List<KeyValue> collectedfrom) {

		// load and parse affiliation relations from HDFS
		Dataset<Row> df = spark
			.read()
			.schema("`DOI` STRING, `Matchings` ARRAY<STRUCT<`RORid`:STRING,`Confidence`:DOUBLE>>")
			.json(inputPath);

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
					+ IdentifierFactory.md5(CleaningFunctions.normalizePidValue("doi", row.getAs("doi")));

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
						BIP_INFERENCE_PROVENANCE,
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
