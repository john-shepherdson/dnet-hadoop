package eu.dnetlib.dhp.oa.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.provision.model.*;
import eu.dnetlib.dhp.oa.provision.utils.ContextMapper;
import eu.dnetlib.dhp.oa.provision.utils.GraphMappingUtils;
import eu.dnetlib.dhp.oa.provision.utils.XmlRecordFactory;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

/**
 * Joins the graph nodes by resolving the links of distance = 1 to create an adjacency list of linked objects.
 * The operation considers all the entity types (publication, dataset, software, ORP, project, datasource, organization,
 * and all the possible relationships (similarity links produced by the Dedup process are excluded).
 *
 * The workflow is organized in different parts aimed to to reduce the complexity of the operation
 *  1) PrepareRelationsJob:
 *      only consider relationships that are not virtually deleted ($.dataInfo.deletedbyinference == false), each entity
 *      can be linked at most to 100 other objects
 *
 *  2) JoinRelationEntityByTargetJob:
 *     (phase 1): prepare tuples [relation - target entity] (R - T):
 *      for each entity type E_i
 *          map E_i as RelatedEntity T_i to simplify the model and extracting only the necessary information
 *          join (R.target = T_i.id)
 *          save the tuples (R_i, T_i)
 *     (phase 2):
 *          create the union of all the entity types E, hash by id
 *          read the tuples (R, T), hash by R.source
 *          join E.id = (R, T).source, where E becomes the Source Entity S
 *          save the tuples (S, R, T)
 *
 *  3) AdjacencyListBuilderJob:
 *      given the tuple (S - R - T) we need to group by S.id -> List [ R - T ], mapping the result as JoinedEntity
 *
 *  4) XmlConverterJob:
 *      convert the JoinedEntities as XML records
 */
public class XmlConverterJob {

    private static final Logger log = LoggerFactory.getLogger(XmlConverterJob.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final String schemaLocation = "https://www.openaire.eu/schema/1.0/oaf-1.0.xsd";

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        XmlConverterJob.class
                                .getResourceAsStream("/eu/dnetlib/dhp/oa/provision/input_params_xml_converter.json")));
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputPath = parser.get("inputPath");
        log.info("inputPath: {}", inputPath);

        String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        String isLookupUrl = parser.get("isLookupUrl");
        log.info("isLookupUrl: {}", isLookupUrl);

        String otherDsTypeId = parser.get("otherDsTypeId");
        log.info("otherDsTypeId: {}", otherDsTypeId);

        SparkConf conf = new SparkConf();

        runWithSparkSession(conf, isSparkSessionManaged,
                spark -> {
                    removeOutputDir(spark, outputPath);
                    convertToXml(spark, inputPath, outputPath, ContextMapper.fromIS(isLookupUrl), otherDsTypeId);
                });

    }

    private static void convertToXml(SparkSession spark, String inputPath, String outputPath, ContextMapper contextMapper, String otherDsTypeId) {

        final XmlRecordFactory recordFactory = new XmlRecordFactory(prepareAccumulators(spark.sparkContext()), contextMapper, false, schemaLocation, otherDsTypeId);

        spark.read()
                .load(inputPath)
                .as(Encoders.bean(JoinedEntity.class))
 /*               .map((MapFunction<JoinedEntity, String>) value -> OBJECT_MAPPER.writeValueAsString(value), Encoders.STRING())
                .write()
                .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                .text("/tmp/json");

        spark.read()
                .textFile("/tmp/json")
                .map((MapFunction<String, JoinedEntity>) value -> OBJECT_MAPPER.readValue(value, JoinedEntity.class), Encoders.bean(JoinedEntity.class))
                .map((MapFunction<JoinedEntity, JoinedEntity>) j -> {
                    if (j.getLinks() != null) {
                        j.setLinks(j.getLinks()
                                .stream()
                                .filter(t -> t.getRelation() != null & t.getRelatedEntity() != null)
                                .collect(Collectors.toCollection(ArrayList::new)));
                    }
                    return j;
                }, Encoders.bean(JoinedEntity.class))

  */
                .map((MapFunction<JoinedEntity, Tuple2<String, String>>) je -> new Tuple2<>(
                        je.getEntity().getId(),
                        recordFactory.build(je)
                ), Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                .javaRDD()
                .mapToPair((PairFunction<Tuple2<String, String>, Text, Text>) t -> new Tuple2<>(new Text(t._1()), new Text(t._2())))
                .saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);
    }

    private static void removeOutputDir(SparkSession spark, String path) {
        HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
    }

    private static Map<String, LongAccumulator> prepareAccumulators(SparkContext sc) {
        Map<String, LongAccumulator> accumulators = Maps.newHashMap();
        accumulators.put("resultResult_similarity_isAmongTopNSimilarDocuments", sc.longAccumulator("resultResult_similarity_isAmongTopNSimilarDocuments"));
        accumulators.put("resultResult_similarity_hasAmongTopNSimilarDocuments", sc.longAccumulator("resultResult_similarity_hasAmongTopNSimilarDocuments"));
        accumulators.put("resultResult_supplement_isSupplementTo", sc.longAccumulator("resultResult_supplement_isSupplementTo"));
        accumulators.put("resultResult_supplement_isSupplementedBy", sc.longAccumulator("resultResult_supplement_isSupplementedBy"));
        accumulators.put("resultResult_dedup_isMergedIn", sc.longAccumulator("resultResult_dedup_isMergedIn"));
        accumulators.put("resultResult_dedup_merges", sc.longAccumulator("resultResult_dedup_merges"));

        accumulators.put("resultResult_publicationDataset_isRelatedTo", sc.longAccumulator("resultResult_publicationDataset_isRelatedTo"));
        accumulators.put("resultResult_relationship_isRelatedTo", sc.longAccumulator("resultResult_relationship_isRelatedTo"));
        accumulators.put("resultProject_outcome_isProducedBy", sc.longAccumulator("resultProject_outcome_isProducedBy"));
        accumulators.put("resultProject_outcome_produces", sc.longAccumulator("resultProject_outcome_produces"));
        accumulators.put("resultOrganization_affiliation_isAuthorInstitutionOf", sc.longAccumulator("resultOrganization_affiliation_isAuthorInstitutionOf"));

        accumulators.put("resultOrganization_affiliation_hasAuthorInstitution", sc.longAccumulator("resultOrganization_affiliation_hasAuthorInstitution"));
        accumulators.put("projectOrganization_participation_hasParticipant", sc.longAccumulator("projectOrganization_participation_hasParticipant"));
        accumulators.put("projectOrganization_participation_isParticipant", sc.longAccumulator("projectOrganization_participation_isParticipant"));
        accumulators.put("organizationOrganization_dedup_isMergedIn", sc.longAccumulator("organizationOrganization_dedup_isMergedIn"));
        accumulators.put("organizationOrganization_dedup_merges", sc.longAccumulator("resultProject_outcome_produces"));
        accumulators.put("datasourceOrganization_provision_isProvidedBy", sc.longAccumulator("datasourceOrganization_provision_isProvidedBy"));
        accumulators.put("datasourceOrganization_provision_provides", sc.longAccumulator("datasourceOrganization_provision_provides"));

        return accumulators;
    }

}
