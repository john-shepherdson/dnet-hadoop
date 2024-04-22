
package eu.dnetlib.dhp.actionmanager.webcrawl;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.dhp.schema.oaf.utils.PidCleaner;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;
import scala.Tuple2;

/**
 * @author miriam.baglioni
 * @Date 18/04/24
 */
public class CreateActionSetFromWebEntries implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(CreateActionSetFromWebEntries.class);
	private static final String DOI_PREFIX = "50|doi_________::";

	private static final String ROR_PREFIX = "20|ror_________::";

	private static final String PMID_PREFIX = "50|pmid________::";

	private static final String PMCID_PREFIX = "50|pmc_________::";
	private static final String WEB_CRAWL_ID = "10|openaire____::fb98a192f6a055ba495ef414c330834b";
	private static final String WEB_CRAWL_NAME = "Web Crawl";
	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				CreateActionSetFromWebEntries.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/webcrawl/as_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {

				createActionSet(spark, inputPath, outputPath + "actionSet");
				createPlainRelations(spark, inputPath, outputPath + "relations");
			});
	}

	private static void createPlainRelations(SparkSession spark, String inputPath, String outputPath) {
		final Dataset<Row> dataset = readWebCrawl(spark, inputPath);

		dataset.flatMap((FlatMapFunction<Row, Tuple2<String, Relation>>) row -> {
			List<Tuple2<String, Relation>> ret = new ArrayList<>();

			final String ror = row.getAs("ror");
			ret.addAll(createAffiliationRelationPairDOI(row.getAs("publication_year"), row.getAs("doi"), ror));
			ret.addAll(createAffiliationRelationPairPMID(row.getAs("publication_year"), row.getAs("pmid"), ror));
			ret.addAll(createAffiliationRelationPairPMCID(row.getAs("publication_year"), row.getAs("pmcid"), ror));

			return ret
				.iterator();
		}, Encoders.tuple(Encoders.STRING(), Encoders.bean(Relation.class)))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static Collection<? extends Tuple2<String, Relation>> createAffiliationRelationPairPMCID(
		String publication_year, String pmcid, String ror) {
		if (pmcid == null)
			return new ArrayList<>();

		return createAffiliatioRelationPair("PMC" + pmcid, ror)
			.stream()
			.map(r -> new Tuple2<String, Relation>(publication_year, r))
			.collect(Collectors.toList());
	}

	private static Collection<? extends Tuple2<String, Relation>> createAffiliationRelationPairPMID(
		String publication_year, String pmid, String ror) {
		if (pmid == null)
			return new ArrayList<>();

		return createAffiliatioRelationPair(pmid, ror)
			.stream()
			.map(r -> new Tuple2<String, Relation>(publication_year, r))
			.collect(Collectors.toList());
	}

	private static Collection<? extends Tuple2<String, Relation>> createAffiliationRelationPairDOI(
		String publication_year, String doi, String ror) {
		if (doi == null)
			return new ArrayList<>();

		return createAffiliatioRelationPair(doi, ror)
			.stream()
			.map(r -> new Tuple2<String, Relation>(publication_year, r))
			.collect(Collectors.toList());
	}

	public static void createActionSet(SparkSession spark, String inputPath,
		String outputPath) {

		final Dataset<Row> dataset = readWebCrawl(spark, inputPath)
				.filter("publication_year <= 2020 or country_code=='IE'")
			.drop("publication_year");

		dataset.flatMap((FlatMapFunction<Row, Relation>) row -> {
			List<Relation> ret = new ArrayList<>();
			final String ror = ROR_PREFIX
				+ IdentifierFactory.md5(PidCleaner.normalizePidValue("ROR", row.getAs("ror")));
			ret.addAll(createAffiliationRelationPairDOI(row.getAs("doi"), ror));
			ret.addAll(createAffiliationRelationPairPMID(row.getAs("pmid"), ror));
			ret.addAll(createAffiliationRelationPairPMCID(row.getAs("pmcid"), ror));

			return ret
				.iterator();
		}, Encoders.bean(Relation.class))
			.toJavaRDD()
			.map(p -> new AtomicAction(p.getClass(), p))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);

	}

	private static Dataset<Row> readWebCrawl(SparkSession spark, String inputPath) {
		StructType webInfo = StructType
			.fromDDL(
				"`id` STRING , `doi` STRING, `ids` STRUCT<`pmid` :STRING, `pmcid`: STRING >, `publication_year` STRING, "
					+
					"`authorships` ARRAY<STRUCT <`institutions`: ARRAY <STRUCT <`ror`: STRING, `country_code` :STRING>>>>");

		return spark
			.read()
			.schema(webInfo)
			.json(inputPath)
			.withColumn(
				"authors", functions
					.explode(
						functions.col("authorships")))
			.selectExpr("id", "doi", "ids", "publication_year", "authors.institutions as institutions")
			.withColumn(
				"institution", functions
					.explode(
						functions.col("institutions")))
			.selectExpr(
				"id", "doi", "ids.pmcid as pmcid", "ids.pmid as pmid", "institution.ror as ror",
				"institution.country_code as country_code", "publication_year")
			// .where("country_code == 'IE'")
			.distinct();

	}

	private static List<Relation> createAffiliationRelationPairPMCID(String pmcid, String ror) {
		if (pmcid == null)
			return new ArrayList<>();

		return createAffiliatioRelationPair(
			PMCID_PREFIX
				+ IdentifierFactory
					.md5(PidCleaner.normalizePidValue(PidType.pmc.toString(), "PMC" + pmcid.substring(43))),
			ror);
	}

	private static List<Relation> createAffiliationRelationPairPMID(String pmid, String ror) {
		if (pmid == null)
			return new ArrayList<>();

		return createAffiliatioRelationPair(
			PMID_PREFIX
				+ IdentifierFactory
					.md5(PidCleaner.normalizePidValue(PidType.pmid.toString(), pmid.substring(33))),
			ror);
	}

	private static List<Relation> createAffiliationRelationPairDOI(String doi, String ror) {
		if (doi == null)
			return new ArrayList<>();

		return createAffiliatioRelationPair(
			DOI_PREFIX
				+ IdentifierFactory
					.md5(PidCleaner.normalizePidValue(PidType.doi.toString(), doi.substring(16))),
			ror);

	}

	private static List<Relation> createAffiliatioRelationPair(String resultId, String orgId) {
		ArrayList<Relation> newRelations = new ArrayList();

		newRelations
			.add(
				OafMapperUtils
					.getRelation(
						orgId, resultId, ModelConstants.RESULT_ORGANIZATION, ModelConstants.AFFILIATION,
						ModelConstants.IS_AUTHOR_INSTITUTION_OF,
						Arrays
							.asList(
								OafMapperUtils.keyValue(WEB_CRAWL_ID, WEB_CRAWL_NAME)),
						OafMapperUtils
							.dataInfo(
								false, null, false, false,
								OafMapperUtils
									.qualifier(
										"sysimport:crasswalk:webcrawl", "Imported from Webcrawl",
										ModelConstants.DNET_PROVENANCE_ACTIONS, ModelConstants.DNET_PROVENANCE_ACTIONS),
								"0.9"),
						null));

		newRelations
			.add(
				OafMapperUtils
					.getRelation(
						resultId, orgId, ModelConstants.RESULT_ORGANIZATION, ModelConstants.AFFILIATION,
						ModelConstants.HAS_AUTHOR_INSTITUTION,
						Arrays
							.asList(
								OafMapperUtils.keyValue(WEB_CRAWL_ID, WEB_CRAWL_NAME)),
						OafMapperUtils
							.dataInfo(
								false, null, false, false,
								OafMapperUtils
									.qualifier(
										"sysimport:crasswalk:webcrawl", "Imported from Webcrawl",
										ModelConstants.DNET_PROVENANCE_ACTIONS, ModelConstants.DNET_PROVENANCE_ACTIONS),
								"0.9"),
						null));

		return newRelations;

	}

}
