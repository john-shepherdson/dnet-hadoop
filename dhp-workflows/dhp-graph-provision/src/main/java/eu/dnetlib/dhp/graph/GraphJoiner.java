package eu.dnetlib.dhp.graph;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import eu.dnetlib.dhp.graph.model.*;
import eu.dnetlib.dhp.graph.utils.ContextMapper;
import eu.dnetlib.dhp.graph.utils.GraphMappingUtils;
import eu.dnetlib.dhp.graph.utils.RelationPartitioner;
import eu.dnetlib.dhp.graph.utils.XmlRecordFactory;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static eu.dnetlib.dhp.graph.utils.GraphMappingUtils.asRelatedEntity;

/**
 * Joins the graph nodes by resolving the links of distance = 1 to create an adjacency list of linked objects.
 * The operation considers all the entity types (publication, dataset, software, ORP, project, datasource, organization,
 * and all the possible relationships (similarity links produced by the Dedup process are excluded).
 *
 * The operation is implemented creating the union between the entity types (E), joined by the relationships (R), and again
 * by E, finally grouped by E.id;
 *
 * Different manipulations of the E and R sets are introduced to reduce the complexity of the operation
 * 1) treat the object payload as string, extracting only the necessary information beforehand using json path,
 *      it seems that deserializing it with jackson's object mapper has higher memory footprint.
 *
 * 2) only consider rels that are not virtually deleted ($.dataInfo.deletedbyinference == false)
 * 3) we only need a subset of fields from the related entities, so we introduce a distinction between E_source = S
 *      and E_target = T. Objects in T are heavily pruned by all the unnecessary information
 *
 * 4) perform the join as (((T.id join R.target) union S) groupby S.id) yield S -> [ <T, R> ]
 */
public class GraphJoiner implements Serializable {

    private Map<String, LongAccumulator> accumulators = Maps.newHashMap();

    public static final int MAX_RELS = 100;

    public static final String schemaLocation = "https://www.openaire.eu/schema/1.0/oaf-1.0.xsd";

    private SparkSession spark;

    private ContextMapper contextMapper;

    private String inputPath;

    private String outPath;

    private String otherDsTypeId;

    public GraphJoiner(SparkSession spark, ContextMapper contextMapper, String otherDsTypeId, String inputPath, String outPath) {
        this.spark = spark;
        this.contextMapper = contextMapper;
        this.otherDsTypeId = otherDsTypeId;
        this.inputPath = inputPath;
        this.outPath = outPath;

        final SparkContext sc = spark.sparkContext();
        prepareAccumulators(sc);
    }

    public GraphJoiner adjacencyLists() {
        final JavaSparkContext jsc = new JavaSparkContext(getSpark().sparkContext());

        // read each entity
        JavaPairRDD<String, TypedRow> datasource = readPathEntity(jsc, getInputPath(), "datasource");
        JavaPairRDD<String, TypedRow> organization = readPathEntity(jsc, getInputPath(), "organization");
        JavaPairRDD<String, TypedRow> project = readPathEntity(jsc, getInputPath(), "project");
        JavaPairRDD<String, TypedRow> dataset = readPathEntity(jsc, getInputPath(), "dataset");
        JavaPairRDD<String, TypedRow> otherresearchproduct = readPathEntity(jsc, getInputPath(), "otherresearchproduct");
        JavaPairRDD<String, TypedRow> software = readPathEntity(jsc, getInputPath(), "software");
        JavaPairRDD<String, TypedRow> publication = readPathEntity(jsc, getInputPath(), "publication");

        // create the union between all the entities
        final String entitiesPath = getOutPath() + "/entities";
        datasource
                .union(organization)
                .union(project)
                .union(dataset)
                .union(otherresearchproduct)
                .union(software)
                .union(publication)
                .map(e -> new EntityRelEntity().setSource(e._2()))
                .map(GraphMappingUtils::serialize)
                .saveAsTextFile(entitiesPath, GzipCodec.class);

        JavaPairRDD<String, EntityRelEntity> entities = jsc.textFile(entitiesPath)
                .map(t -> new ObjectMapper().readValue(t, EntityRelEntity.class))
                .mapToPair(t -> new Tuple2<>(t.getSource().getSourceId(), t));

        final String relationPath = getOutPath() + "/relation";
        // reads the relationships
        final JavaPairRDD<SortableRelationKey, EntityRelEntity> rels = readPathRelation(jsc, getInputPath())
                .filter(rel -> !rel.getDeleted()) //only consider those that are not virtually deleted
                .map(p -> new EntityRelEntity().setRelation(p))
                .mapToPair(p -> new Tuple2<>(SortableRelationKey.from(p), p));
        rels
                .groupByKey(new RelationPartitioner(rels.getNumPartitions()))
                .map(p -> Iterables.limit(p._2(), MAX_RELS))
                .flatMap(p -> p.iterator())
                .map(s -> new ObjectMapper().writeValueAsString(s))
                .saveAsTextFile(relationPath, GzipCodec.class);

        final JavaPairRDD<String, EntityRelEntity> relation = jsc.textFile(relationPath)
                .map(s -> new ObjectMapper().readValue(s, EntityRelEntity.class))
                .mapToPair(p -> new Tuple2<>(p.getRelation().getTargetId(), p));

        final String bySourcePath = getOutPath() + "/join_by_source";
        relation
                .join(entities
                        .filter(e -> !e._2().getSource().getDeleted())
                        .mapToPair(e -> new Tuple2<>(e._1(), asRelatedEntity(e._2()))))
                .map(s -> new EntityRelEntity()
                        .setRelation(s._2()._1().getRelation())
                        .setTarget(s._2()._2().getSource()))
                .map(j -> new ObjectMapper().writeValueAsString(j))
                .saveAsTextFile(bySourcePath, GzipCodec.class);

        JavaPairRDD<String, EntityRelEntity> bySource = jsc.textFile(bySourcePath)
                .map(e -> getObjectMapper().readValue(e, EntityRelEntity.class))
                .mapToPair(t -> new Tuple2<>(t.getRelation().getSourceId(), t));

        final XmlRecordFactory recordFactory = new XmlRecordFactory(accumulators, contextMapper, false, schemaLocation, otherDsTypeId);
        entities
                .union(bySource)
                .groupByKey()   // by source id
                .map(l -> toJoinedEntity(l))
                .mapToPair(je -> new Tuple2<>(
                        new Text(je.getEntity().getId()),
                        new Text(recordFactory.build(je))))
                .saveAsHadoopFile(getOutPath() + "/xml", Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);

        return this;
    }

    public SparkSession getSpark() {
        return spark;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getOutPath() {
        return outPath;
    }

    // HELPERS

    private OafEntity parseOaf(final String json, final String type, final ObjectMapper mapper) {
        try {
            switch (GraphMappingUtils.EntityType.valueOf(type)) {
                case publication:
                    return mapper.readValue(json, Publication.class);
                case dataset:
                    return mapper.readValue(json, Dataset.class);
                case otherresearchproduct:
                    return mapper.readValue(json, OtherResearchProduct.class);
                case software:
                    return mapper.readValue(json, Software.class);
                case datasource:
                    return mapper.readValue(json, Datasource.class);
                case organization:
                    return mapper.readValue(json, Organization.class);
                case project:
                    return mapper.readValue(json, Project.class);
                default:
                    throw new IllegalArgumentException("invalid type: " + type);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private JoinedEntity toJoinedEntity(Tuple2<String, Iterable<EntityRelEntity>> p) {
        final ObjectMapper mapper = getObjectMapper();
        final JoinedEntity j = new JoinedEntity();
        final Links links = new Links();
        for(EntityRelEntity rel : p._2()) {
            if (rel.hasMainEntity() & j.getEntity() == null) {
                j.setType(rel.getSource().getType());
                j.setEntity(parseOaf(rel.getSource().getOaf(), rel.getSource().getType(), mapper));
            }
            if (rel.hasRelatedEntity()) {
                try {
                    links.add(
                            new eu.dnetlib.dhp.graph.model.Tuple2()
                                    .setRelation(mapper.readValue(rel.getRelation().getOaf(), Relation.class))
                                    .setRelatedEntity(mapper.readValue(rel.getTarget().getOaf(), RelatedEntity.class)));
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        }
        j.setLinks(links);
        if (j.getEntity() == null) {
            throw new IllegalStateException("missing main entity on '" + p._1() + "'");
        }
        return j;
    }

    /**
     * Reads a set of eu.dnetlib.dhp.schema.oaf.OafEntity objects from a sequence file <className, entity json serialization>,
     * extracts necessary information using json path, wraps the oaf object in a eu.dnetlib.dhp.graph.model.TypedRow
     * @param sc
     * @param inputPath
     * @param type
     * @return the JavaPairRDD<String, TypedRow> indexed by entity identifier
     */
    private JavaPairRDD<String, TypedRow> readPathEntity(final JavaSparkContext sc, final String inputPath, final String type) {
        return sc.sequenceFile(inputPath + "/" + type, Text.class, Text.class)
                .mapToPair((PairFunction<Tuple2<Text, Text>, String, TypedRow>) item -> {
                    final String s = item._2().toString();
                    final DocumentContext json = JsonPath.parse(s);
                    final String id = json.read("$.id");
                    return new Tuple2<>(id, new TypedRow()
                            .setSourceId(id)
                            .setDeleted(json.read("$.dataInfo.deletedbyinference"))
                            .setType(type)
                            .setOaf(s));
                });
    }

    /**
     * Reads a set of eu.dnetlib.dhp.schema.oaf.Relation objects from a sequence file <className, relation json serialization>,
     * extracts necessary information using json path, wraps the oaf object in a eu.dnetlib.dhp.graph.model.TypedRow
     * @param sc
     * @param inputPath
     * @return the JavaRDD<TypedRow> containing all the relationships
     */
    private JavaRDD<TypedRow> readPathRelation(final JavaSparkContext sc, final String inputPath) {
        return sc.sequenceFile(inputPath + "/relation", Text.class, Text.class)
                .map(item -> {
                    final String s = item._2().toString();
                    final DocumentContext json = JsonPath.parse(s);
                    return new TypedRow()
                            .setSourceId(json.read("$.source"))
                            .setTargetId(json.read("$.target"))
                            .setDeleted(json.read("$.dataInfo.deletedbyinference"))
                            .setType("relation")
                            .setRelType("$.relType")
                            .setSubRelType("$.subRelType")
                            .setRelClass("$.relClass")
                            .setOaf(s);
                });
    }

    private ObjectMapper getObjectMapper() {
        return new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private void prepareAccumulators(SparkContext sc) {
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
    }

}
