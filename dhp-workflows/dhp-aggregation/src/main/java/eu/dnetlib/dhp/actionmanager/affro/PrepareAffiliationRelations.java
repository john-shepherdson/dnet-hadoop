
package eu.dnetlib.dhp.actionmanager.affro;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.functions.expr;

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
import org.apache.spark.sql.types.DataTypes;
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

	private static final Object OPENORGS_NS_PREFIX = "openorgs____";

	public static <I extends Result> void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareAffiliationRelations.class
					.getResourceAsStream(
							"/eu/dnetlib/dhp/actionmanager/affro/input_actionset_parameter.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Constants.isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String matchingPath = parser.get("matchingPath");
		log.info("matchingPath: {}", matchingPath);

		final String openapcInputPath = parser.get("openapcInputPath");
		log.info("openapcInputPath: {}", openapcInputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Constants.removeOutputDir(spark, outputPath);
				createActionSet(
					spark, matchingPath, openapcInputPath,  outputPath);
			});
	}

	private static void createActionSet(SparkSession spark, String matchingPath,
		String openapcInputPath, String outputPath) {
		List<KeyValue> collectedfromOpenAIRE = OafMapperUtils
			.listKeyValues(OPENAIRE_DATASOURCE_ID, OPENAIRE_DATASOURCE_NAME);

		JavaPairRDD<Text, Text> relationsNewModel = prepareAffiliationRelationFromPublisherNewModel(
			spark, matchingPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE );
		JavaPairRDD<Text, Text> openAPCRelations = prepareAffiliationRelationsNewModel(
				spark, openapcInputPath, collectedfromOpenAIRE, BIP_INFERENCE_PROVENANCE );

		relationsNewModel
				.union(openAPCRelations)
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
				"`id` STRING, `Organizations` ARRAY<STRUCT<`PID`:STRING, `Value`:STRING,`Confidence`:DOUBLE, `Status`:STRING>>")
			.json(inputPath)
			.where("id is not null");

		return getTextTextJavaPairRDDNew(
			collectedfrom, df.selectExpr("id", "Organizations as Matchings"), dataprovenance);

	}

	private static <I extends Result> JavaPairRDD<Text, Text> prepareAffiliationRelationsNewModel(SparkSession spark,
																								  String inputPath,
																								  List<KeyValue> collectedfrom, String dataprovenance) {

		spark
				.udf()
				.register(
						"md5HashWithPrefix", (String doi) -> ID_PREFIX + DHPUtils.md5(doi), DataTypes.StringType);

		// load and parse affiliation relations from HDFS
		Dataset<Row> df = spark
				.read()
				.schema(
						"`DOI` STRING, `Matchings` ARRAY<STRUCT<`PID`:STRING, `Value`:STRING,`Confidence`:DOUBLE, `Status`:STRING>>")
				.json(inputPath)
				.where("DOI is not null")
				.withColumn("id",  expr("md5HashWithPrefix(DOI)"))
				.drop("DOI");

		return getTextTextJavaPairRDDNew(collectedfrom, df, dataprovenance);
	}

	private static JavaPairRDD<Text, Text> getTextTextJavaPairRDDNew(List<KeyValue> collectedfrom, Dataset<Row> df,
		String dataprovenance) {
		// unroll nested arrays
		df = df
			.withColumn("matching", functions.explode(new Column("Matchings")))
			.select(
				new Column("id").as("id"),
				new Column("matching.PID").as("pidtype"),
				new Column("matching.Value").as("pidvalue"),
				new Column("matching.Confidence").as("confidence"),
				new Column("matching.Status").as("status"))
			.where("status = 'active'");

		// prepare action sets for affiliation relations
		return df
			.toJavaRDD()
			.flatMap((FlatMapFunction<Row, Relation>) row -> {
				String paperId = row.getAs("id");
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
