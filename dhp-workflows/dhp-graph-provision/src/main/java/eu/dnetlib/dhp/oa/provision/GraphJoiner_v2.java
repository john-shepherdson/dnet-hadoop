package eu.dnetlib.dhp.oa.provision;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import eu.dnetlib.dhp.oa.provision.model.*;
import eu.dnetlib.dhp.oa.provision.utils.*;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.*;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static eu.dnetlib.dhp.oa.provision.utils.GraphMappingUtils.asRelatedEntity;

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
public class GraphJoiner_v2 implements Serializable {

    private Map<String, LongAccumulator> accumulators = Maps.newHashMap();

    public static final int MAX_RELS = 100;

    public static final String schemaLocation = "https://www.openaire.eu/schema/1.0/oaf-1.0.xsd";

    private SparkSession spark;

    private ContextMapper contextMapper;

    private String inputPath;

    private String outPath;

    private String otherDsTypeId;

    public GraphJoiner_v2(SparkSession spark, ContextMapper contextMapper, String otherDsTypeId, String inputPath, String outPath) {
        this.spark = spark;
        this.contextMapper = contextMapper;
        this.otherDsTypeId = otherDsTypeId;
        this.inputPath = inputPath;
        this.outPath = outPath;

        final SparkContext sc = spark.sparkContext();
        prepareAccumulators(sc);
    }

    public GraphJoiner_v2 adjacencyLists() throws IOException {

        final JavaSparkContext jsc = JavaSparkContext.fromSparkContext(getSpark().sparkContext());

        // read each entity
        Dataset<TypedRow> datasource = readPathEntity(jsc, getInputPath(), "datasource");
        Dataset<TypedRow> organization = readPathEntity(jsc, getInputPath(), "organization");
        Dataset<TypedRow> project = readPathEntity(jsc, getInputPath(), "project");
        Dataset<TypedRow> dataset = readPathEntity(jsc, getInputPath(), "dataset");
        Dataset<TypedRow> otherresearchproduct = readPathEntity(jsc, getInputPath(), "otherresearchproduct");
        Dataset<TypedRow> software = readPathEntity(jsc, getInputPath(), "software");
        Dataset<TypedRow> publication = readPathEntity(jsc, getInputPath(), "publication");

        // create the union between all the entities
        Dataset<Tuple2<String, TypedRow>> entities =
                datasource
                        .union(organization)
                        .union(project)
                        .union(dataset)
                        .union(otherresearchproduct)
                        .union(software)
                        .union(publication)
                .map((MapFunction<TypedRow, Tuple2<String, TypedRow>>) value -> new Tuple2<>(
                        value.getId(),
                        value),
                        Encoders.tuple(Encoders.STRING(), Encoders.kryo(TypedRow.class)))
                .cache();

        System.out.println("Entities schema:");
        entities.printSchema();
        // reads the relationships

        Dataset<Relation> rels = readPathRelation(jsc, getInputPath())
                .groupByKey((MapFunction<Relation, SortableRelationKey>) t -> SortableRelationKey.from(t), Encoders.kryo(SortableRelationKey.class))
                .flatMapGroups((FlatMapGroupsFunction<SortableRelationKey, Relation, Relation>) (key, values) -> Iterators.limit(values, MAX_RELS), Encoders.bean(Relation.class))
                .cache();

        System.out.println("Relation schema:");
        rels.printSchema();

        Dataset<Tuple2<String, Relation>> relsByTarget = rels
                .map((MapFunction<Relation, Tuple2<String, Relation>>) r -> new Tuple2<>(r.getTarget(), r), Encoders.tuple(Encoders.STRING(), Encoders.kryo(Relation.class)));

        System.out.println("Relation by target schema:");
        relsByTarget.printSchema();

        Dataset<Tuple2<String, EntityRelEntity>> bySource = relsByTarget
                .joinWith(entities, relsByTarget.col("_1").equalTo(entities.col("_1")), "inner")
                .filter((FilterFunction<Tuple2<Tuple2<String, Relation>, Tuple2<String, TypedRow>>>) value -> value._2()._2().getDeleted() == false)
                .map((MapFunction<Tuple2<Tuple2<String, Relation>, Tuple2<String, TypedRow>>, EntityRelEntity>) t -> {
                    EntityRelEntity e = new EntityRelEntity();
                    e.setRelation(t._1()._2());
                    e.setTarget(asRelatedEntity(t._2()._2()));
                    return e;
                }, Encoders.bean(EntityRelEntity.class))
                .map((MapFunction<EntityRelEntity, Tuple2<String, EntityRelEntity>>) e -> new Tuple2<>(e.getRelation().getSource(), e),
                        Encoders.tuple(Encoders.STRING(), Encoders.kryo(EntityRelEntity.class)));

        System.out.println("bySource schema");
        bySource.printSchema();

        Dataset<EntityRelEntity> joined = entities
                .joinWith(bySource, entities.col("_1").equalTo(bySource.col("_1")), "left")
                .map((MapFunction<Tuple2<Tuple2<String, TypedRow>, Tuple2<String, EntityRelEntity>>, EntityRelEntity>) value -> {
                    EntityRelEntity re = new EntityRelEntity();
                    re.setEntity(value._1()._2());
                    Optional<EntityRelEntity> related = Optional.ofNullable(value._2()).map(Tuple2::_2);
                    if (related.isPresent()) {
                        re.setRelation(related.get().getRelation());
                        re.setTarget(related.get().getTarget());
                    }
                    return re;
                }, Encoders.kryo(EntityRelEntity.class));

        System.out.println("joined schema");
        joined.printSchema();
        //joined.write().json(getOutPath() + "/joined");

        final Dataset<JoinedEntity> grouped = joined
                .groupByKey((MapFunction<EntityRelEntity, TypedRow>) e -> e.getEntity(), Encoders.kryo(TypedRow.class))
                .mapGroups((MapGroupsFunction<TypedRow, EntityRelEntity, JoinedEntity>) (key, values) -> toJoinedEntity(key, values), Encoders.kryo(JoinedEntity.class));

        System.out.println("grouped schema");
        grouped.printSchema();

        final XmlRecordFactory recordFactory = new XmlRecordFactory(accumulators, contextMapper, false, schemaLocation, otherDsTypeId);
        grouped
                .map((MapFunction<JoinedEntity, String>) value -> recordFactory.build(value), Encoders.STRING())
                .write()
                .text(getOutPath() + "/xml");
        /*
                .javaRDD()
                .mapToPair((PairFunction<Tuple2<String, String>, String, String>) t -> new Tuple2<>(t._1(), t._2()))
                .saveAsHadoopFile(getOutPath() + "/xml", Text.class, Text.class, SequenceFileOutputFormat.class, GzipCodec.class);

         */

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

    private JoinedEntity toJoinedEntity(TypedRow key, Iterator<EntityRelEntity> values) {
        final ObjectMapper mapper = getObjectMapper();
        final JoinedEntity j = new JoinedEntity();
        j.setType(key.getType());
        j.setEntity(parseOaf(key.getOaf(), key.getType(), mapper));
        final Links links = new Links();
        values.forEachRemaining(rel -> links.add(
                new eu.dnetlib.dhp.oa.provision.model.Tuple2(
                        rel.getRelation(),
                        rel.getTarget()
                )));
        j.setLinks(links);
        return j;
    }

    private OafEntity parseOaf(final String json, final String type, final ObjectMapper mapper) {
        try {
            switch (GraphMappingUtils.EntityType.valueOf(type)) {
                case publication:
                    return mapper.readValue(json, Publication.class);
                case dataset:
                    return mapper.readValue(json, eu.dnetlib.dhp.schema.oaf.Dataset.class);
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

    /**
     * Reads a set of eu.dnetlib.dhp.schema.oaf.OafEntity objects from a new line delimited json file,
     * extracts necessary information using json path, wraps the oaf object in a eu.dnetlib.dhp.graph.model.TypedRow
     * @param sc
     * @param inputPath
     * @param type
     * @return the JavaPairRDD<String, TypedRow> indexed by entity identifier
     */
    private Dataset<TypedRow> readPathEntity(final JavaSparkContext sc, final String inputPath, final String type) {
        RDD<String> rdd = sc.textFile(inputPath + "/" + type)
                .rdd();

        return getSpark().createDataset(rdd, Encoders.STRING())
                .map((MapFunction<String, TypedRow>) s -> {
                    final DocumentContext json = JsonPath.parse(s);
                    final TypedRow t = new TypedRow();
                    t.setId(json.read("$.id"));
                    t.setDeleted(json.read("$.dataInfo.deletedbyinference"));
                    t.setType(type);
                    t.setOaf(s);
                    return t;
                }, Encoders.bean(TypedRow.class));
    }

    /**
     * Reads a set of eu.dnetlib.dhp.schema.oaf.Relation objects from a sequence file <className, relation json serialization>,
     * extracts necessary information using json path, wraps the oaf object in a eu.dnetlib.dhp.graph.model.TypedRow
     * @param sc
     * @param inputPath
     * @return the JavaRDD<TypedRow> containing all the relationships
     */
    private Dataset<Relation> readPathRelation(final JavaSparkContext sc, final String inputPath) {
        final RDD<String> rdd = sc.textFile(inputPath + "/relation")
                .rdd();

        return getSpark().createDataset(rdd, Encoders.STRING())
                .map((MapFunction<String, Relation>) s -> new ObjectMapper().readValue(s, Relation.class), Encoders.bean(Relation.class));
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
