
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.utils.DHPUtils.toSeq;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.oa.provision.model.JoinedEntity;
import eu.dnetlib.dhp.oa.provision.model.ProvisionModelSupport;
import eu.dnetlib.dhp.oa.provision.model.TupleWrapper;
import eu.dnetlib.dhp.oa.provision.utils.ContextMapper;
import eu.dnetlib.dhp.oa.provision.utils.XmlRecordFactory;
import eu.dnetlib.dhp.schema.solr.SolrRecord;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import scala.Tuple2;

/**
 * XmlConverterJob converts the JoinedEntities as XML records
 */
public class PayloadConverterJob {

	private static final Logger log = LoggerFactory.getLogger(PayloadConverterJob.class);

	public static final String schemaLocation = "https://www.openaire.eu/schema/1.0/oaf-1.0.xsd";

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					PayloadConverterJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/provision/input_params_payload_converter.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final Boolean validateXML = Optional
				.ofNullable(parser.get("validateXML"))
				.map(Boolean::valueOf)
				.orElse(Boolean.FALSE);
		log.info("validateXML: {}", validateXML);

		final String contextApiBaseUrl = parser.get("contextApiBaseUrl");
		log.info("contextApiBaseUrl: {}", contextApiBaseUrl);

		String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		ISLookUpService isLookup = ISLookupClientFactory.getLookUpService(isLookupUrl);

		final SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ProvisionModelSupport.getModelClasses());

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			removeOutputDir(spark, outputPath);
			createPayloads(
				spark, inputPath, outputPath, ContextMapper.fromAPI(contextApiBaseUrl),
				VocabularyGroup.loadVocsFromIS(isLookup), validateXML);
		});
	}

	private static void createPayloads(
		final SparkSession spark,
		final String inputPath,
		final String outputPath,
		final ContextMapper contextMapper,
		final VocabularyGroup vocabularies,
		final Boolean validateXML) {

		final XmlRecordFactory recordFactory = new XmlRecordFactory(
			prepareAccumulators(spark.sparkContext()),
			contextMapper,
			false,
			schemaLocation);

		final List<String> paths = HdfsSupport
			.listFiles(inputPath, spark.sparkContext().hadoopConfiguration());

		log.info("Found paths: {}", String.join(",", paths));

		final ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
		spark
			.read()
			.load(toSeq(paths))
			.as(Encoders.kryo(JoinedEntity.class))
			.map(
				(MapFunction<JoinedEntity, Tuple2<String, SolrRecord>>) je -> new Tuple2<>(
					recordFactory.build(je, validateXML),
					ProvisionModelSupport.transform(je, contextMapper, vocabularies)),
				Encoders.tuple(Encoders.STRING(), Encoders.bean(SolrRecord.class)))
			.map(
				(MapFunction<Tuple2<String, SolrRecord>, TupleWrapper>) t -> new TupleWrapper(
					t._1(), mapper.writeValueAsString(t._2())),
				Encoders.bean(TupleWrapper.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static void removeOutputDir(final SparkSession spark, final String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	private static Map<String, LongAccumulator> prepareAccumulators(final SparkContext sc) {
		final Map<String, LongAccumulator> accumulators = Maps.newHashMap();
		accumulators
			.put(
				"resultResult_similarity_isAmongTopNSimilarDocuments",
				sc.longAccumulator("resultResult_similarity_isAmongTopNSimilarDocuments"));
		accumulators
			.put(
				"resultResult_similarity_hasAmongTopNSimilarDocuments",
				sc.longAccumulator("resultResult_similarity_hasAmongTopNSimilarDocuments"));
		accumulators
			.put(
				"resultResult_supplement_isSupplementTo", sc.longAccumulator("resultResult_supplement_isSupplementTo"));
		accumulators
			.put(
				"resultResult_supplement_isSupplementedBy",
				sc.longAccumulator("resultResult_supplement_isSupplementedBy"));
		accumulators
			.put("resultResult_dedup_isMergedIn", sc.longAccumulator("resultResult_dedup_isMergedIn"));
		accumulators.put("resultResult_dedup_merges", sc.longAccumulator("resultResult_dedup_merges"));

		accumulators
			.put(
				"resultResult_publicationDataset_isRelatedTo",
				sc.longAccumulator("resultResult_publicationDataset_isRelatedTo"));
		accumulators
			.put("resultResult_relationship_isRelatedTo", sc.longAccumulator("resultResult_relationship_isRelatedTo"));
		accumulators
			.put("resultProject_outcome_isProducedBy", sc.longAccumulator("resultProject_outcome_isProducedBy"));
		accumulators
			.put("resultProject_outcome_produces", sc.longAccumulator("resultProject_outcome_produces"));
		accumulators
			.put(
				"resultOrganization_affiliation_isAuthorInstitutionOf",
				sc.longAccumulator("resultOrganization_affiliation_isAuthorInstitutionOf"));

		accumulators
			.put(
				"resultOrganization_affiliation_hasAuthorInstitution",
				sc.longAccumulator("resultOrganization_affiliation_hasAuthorInstitution"));
		accumulators
			.put(
				"projectOrganization_participation_hasParticipant",
				sc.longAccumulator("projectOrganization_participation_hasParticipant"));
		accumulators
			.put(
				"projectOrganization_participation_isParticipant",
				sc.longAccumulator("projectOrganization_participation_isParticipant"));
		accumulators
			.put(
				"organizationOrganization_dedup_isMergedIn",
				sc.longAccumulator("organizationOrganization_dedup_isMergedIn"));
		accumulators
			.put("organizationOrganization_dedup_merges", sc.longAccumulator("resultProject_outcome_produces"));
		accumulators
			.put(
				"datasourceOrganization_provision_isProvidedBy",
				sc.longAccumulator("datasourceOrganization_provision_isProvidedBy"));
		accumulators
			.put(
				"datasourceOrganization_provision_provides",
				sc.longAccumulator("datasourceOrganization_provision_provides"));

		return accumulators;
	}
}
