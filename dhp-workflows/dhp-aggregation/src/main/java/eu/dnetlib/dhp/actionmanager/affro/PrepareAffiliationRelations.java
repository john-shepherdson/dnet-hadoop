
package eu.dnetlib.dhp.actionmanager.affro;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.functions.expr;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.flatbuf.Bool;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
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
	public static final String AFFILIATION_INFERENCE_PROVENANCE = "openaire:affiliation";
	public static final String OPENAIRE_DATASOURCE_ID = "10|infrastruct_::f66f1bd369679b5b077dcdf006089556";
	public static final String OPENAIRE_DATASOURCE_NAME = "OpenAIRE";
	public static final String DOI_URL_PREFIX = "https://doi.org/";
	public static final int DOI_URL_PREFIX_LENGTH = 16;
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

		final String openapcInputPath = parser.get("openapcInputPath");
		log.info("openapcInputPath: {}", openapcInputPath);

		final String inputPaths = parser.get("inputPaths");
		log.info("inputPaths: {}", inputPaths);

		final Boolean importIIS = Optional.ofNullable(parser.get("importIIS"))
				.map(Boolean::valueOf)
				.orElse(Boolean.FALSE);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Constants.removeOutputDir(spark, outputPath);
				createActionSet(
					spark,  openapcInputPath,  inputPaths,
					 outputPath, importIIS);
			});
	}

	private static void createActionSet(SparkSession spark,
		String openapcInputPath, String inputPaths, String outputPath, Boolean importIIS) {

		List<KeyValue> collectedfromOpenAIRE = OafMapperUtils
			.listKeyValues(OPENAIRE_DATASOURCE_ID, OPENAIRE_DATASOURCE_NAME);

		Dataset<Relation> crossrefRelations = prepareAffiliationRelationsGraph(
			spark, inputPaths + "/crossref", collectedfromOpenAIRE, AFFILIATION_INFERENCE_PROVENANCE + ":crossref");

		Dataset<Relation> pubmedRelations = prepareAffiliationRelationsGraph(
			spark, inputPaths + "/pubmed", collectedfromOpenAIRE, AFFILIATION_INFERENCE_PROVENANCE + ":pubmed");

		Dataset<Relation> openAPCRelations = prepareAffiliationRelationsNewModel(
			spark, openapcInputPath, collectedfromOpenAIRE, AFFILIATION_INFERENCE_PROVENANCE + ":openapc");

		Dataset<Relation> dataciteRelations = prepareAffiliationRelationsGraph(
			spark, inputPaths + "/datacite", collectedfromOpenAIRE, AFFILIATION_INFERENCE_PROVENANCE + ":datacite");

		Dataset<Relation> oalexRelations = prepareAffiliationRelationsGraph(
			spark, inputPaths + "/oalex", collectedfromOpenAIRE, AFFILIATION_INFERENCE_PROVENANCE + ":rawaff");

		Dataset<Relation> publisherRelations = prepareAffiliationRelationsGraph(
			spark, inputPaths + "/publishers", collectedfromOpenAIRE, AFFILIATION_INFERENCE_PROVENANCE + ":webcrawl");

		Dataset<Relation> oaireRelations = prepareAffiliationRelationsGraph(
				spark, inputPaths + "/oaire", collectedfromOpenAIRE, AFFILIATION_INFERENCE_PROVENANCE + ":graph");

		Dataset<Relation> iisRelations = spark.createDataset(Collections.emptyList(), Encoders.bean(Relation.class));
		if (importIIS)
			iisRelations = prepareAffiliationRelationsGraph(spark,
					inputPaths + "/iis", collectedfromOpenAIRE, AFFILIATION_INFERENCE_PROVENANCE + ":iis");

		crossrefRelations
			.union(pubmedRelations)
			.union(openAPCRelations)
			.union(dataciteRelations)
			.union(oalexRelations)
			.union(publisherRelations)
				.union(oaireRelations)
				.union(iisRelations)
				.groupByKey((MapFunction<Relation, String>) r -> r.getSource() + "::" + r.getRelClass() + "::" + r.getTarget(), Encoders.STRING() )
				.mapGroups((MapGroupsFunction<String, Relation, Relation>) (k,it) -> it.next(), Encoders.bean(Relation.class) )
				.toJavaRDD()
				.map(p -> new AtomicAction(Relation.class, p))
				.mapToPair(
						aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
								new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(
				outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, BZip2Codec.class);
	}

	private static List<Relation> getRelationPairs(List<KeyValue> collectedfrom, String resultid,
												   String organizationid, String organizationpid, String dataprovenance,
												   Double confidence){
		String affId = null;
		if (organizationpid.equalsIgnoreCase("ROR"))
			// ROR id to OpenIARE id
			affId = GenerateRorActionSetJob.calculateOpenaireId(organizationid);
		else
			// getting the OpenOrgs identifier for the organization
			affId = calculateOpenOrgsId(organizationid);

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
						Double.toString(confidence));

		// return bi-directional relations
		return getAffiliationRelationPair(resultid, affId, collectedfrom, dataInfo);

	}

	private static Dataset<Relation> getRels(Dataset<Row> inputDataset, List<KeyValue> collectedfromOpenAIRE, String dataprovenance){
		return inputDataset.select(new Column("id").as("id"),
						new Column("matching.pid").as("pidtype"),
						new Column("matching.value").as("pidvalue"),
						new Column("matching.confidence").as("confidence"),
						new Column("matching.status").as("status"),
						new Column("matching.name").as("name"),
						new Column("matching.country").as("country"))
				.where("status = 'active'").flatMap((FlatMapFunction<Row, Relation>) r -> getRelationPairs(collectedfromOpenAIRE, r.getAs("id"),
						r.getAs("pidvalue"), r.getAs("pidtype"), dataprovenance,
						r.getAs("confidence")).iterator(), Encoders.bean(Relation.class));
	}

	private static Dataset<Relation> prepareAffiliationRelationsGraph(SparkSession spark, String datasetPath, List<KeyValue> collectedfromOpenAIRE, String dataprovenance) {
		return  getRels(spark.read().schema(eu.dnetlib.dhp.actionmanager.affro.Constants.RESULT_MATCHED_SCHEMA).json(datasetPath)
				.select("id","organizations")
				.withColumn("matching", functions.explode(new Column("organizations"))), collectedfromOpenAIRE, dataprovenance);

	}

	private static Dataset<Relation> prepareAffiliationRelationsNewModel(SparkSession spark,
		String inputPath,
		List<KeyValue> collectedfrom, String dataprovenance) {

		spark
				.udf()
				.register(
						"md5HashWithPrefix", (String doi) -> ID_PREFIX + IdentifierFactory.md5(DoiCleaningRule.clean(removePrefix(doi))), DataTypes.StringType);
		// load and parse affiliation relations from HDFS
		return getRels( spark
			.read()
			.schema(eu.dnetlib.dhp.actionmanager.affro.Constants.OPENAPC_INPUT_SCHEMA)
			.json(inputPath)
			.where("doi is not null")
				.withColumn("id",  expr("md5HashWithPrefix(doi)"))
				.withColumn("matching", functions.explode(new Column("matchings"))), collectedfrom, dataprovenance );


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
