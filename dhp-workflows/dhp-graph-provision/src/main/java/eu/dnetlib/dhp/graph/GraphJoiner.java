package eu.dnetlib.dhp.graph;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

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
 * 4) perform the join as (((T join R) union S) groupby S.id) yield S -> [ <T, R> ]
 */
public class GraphJoiner implements Serializable {

    public static final int MAX_RELS = 10;

    private SparkSession spark;

    private String inputPath;

    private String outPath;

    public GraphJoiner(SparkSession spark, String inputPath, String outPath) {
        this.spark = spark;
        this.inputPath = inputPath;
        this.outPath = outPath;
    }

    public GraphJoiner adjacencyLists() {
        final JavaSparkContext sc = new JavaSparkContext(getSpark().sparkContext());

        // read each entity
        JavaPairRDD<String, TypedRow> datasource = readPathEntity(sc, getInputPath(), "datasource");
        JavaPairRDD<String, TypedRow> organization = readPathEntity(sc, getInputPath(), "organization");
        JavaPairRDD<String, TypedRow> project = readPathEntity(sc, getInputPath(), "project");
        JavaPairRDD<String, TypedRow> dataset = readPathEntity(sc, getInputPath(), "dataset");
        JavaPairRDD<String, TypedRow> otherresearchproduct = readPathEntity(sc, getInputPath(), "otherresearchproduct");
        JavaPairRDD<String, TypedRow> software = readPathEntity(sc, getInputPath(), "software");
        JavaPairRDD<String, TypedRow> publication = readPathEntity(sc, getInputPath(), "publication");

        // create the union between all the entities
        final String entitiesPath = getOutPath() + "/0_entities";
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

        JavaPairRDD<String, EntityRelEntity> entities = sc.textFile(entitiesPath)
                .map(t -> new ObjectMapper().readValue(t, EntityRelEntity.class))
                .mapToPair(t -> new Tuple2<>(t.getSource().getSourceId(), t));

        // reads the relationships
        final JavaPairRDD<String, EntityRelEntity> relation = readPathRelation(sc, getInputPath())
                .filter(r -> !r.getDeleted()) //only consider those that are not virtually deleted
                .map(p -> new EntityRelEntity().setRelation(p))
                .mapToPair(p -> new Tuple2<>(p.getRelation().getSourceId(), p))
                .groupByKey()
                .map(p -> Iterables.limit(p._2(), MAX_RELS))
                .flatMap(p -> p.iterator())
                .mapToPair(p -> new Tuple2<>(p.getRelation().getTargetId(), p));

        final String joinByTargetPath = getOutPath() + "/1_join_by_target";
        relation
                .join(entities
                        .filter(e -> !e._2().getSource().getDeleted())
                        .mapToPair(e -> new Tuple2<>(e._1(), new GraphMappingUtils().pruneModel(e._2()))))
                .map(s -> new EntityRelEntity()
                        .setRelation(s._2()._1().getRelation())
                        .setTarget(s._2()._2().getSource()))
                .map(GraphMappingUtils::serialize)
                .saveAsTextFile(joinByTargetPath, GzipCodec.class);

        JavaPairRDD<String, EntityRelEntity> bySource = sc.textFile(joinByTargetPath)
                .map(t -> new ObjectMapper().readValue(t, EntityRelEntity.class))
                .mapToPair(t -> new Tuple2<>(t.getRelation().getSourceId(), t));

        final String linkedEntityPath = getOutPath() + "/2_linked_entities";
        entities
                .union(bySource)
                .groupByKey()   // by source id
                .map(p -> toLinkedEntity(p))
                .map(e -> new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL).writeValueAsString(e))
                .saveAsTextFile(linkedEntityPath, GzipCodec.class);

        final String joinedEntitiesPath = getOutPath() + "/3_joined_entities";
        sc.textFile(linkedEntityPath)
                .map(s -> new ObjectMapper().readValue(s, LinkedEntity.class))
                .map(l -> toJoinedEntity(l))
                .map(j -> new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL).writeValueAsString(j))
                .saveAsTextFile(joinedEntitiesPath);

        return this;
    }

    public GraphJoiner asXML() {
        final JavaSparkContext sc = new JavaSparkContext(getSpark().sparkContext());

        final String joinedEntitiesPath = getOutPath() + "/3_joined_entities";
        sc.textFile(joinedEntitiesPath)
                .map(s -> new ObjectMapper().readValue(s, LinkedEntity.class))
                .map(l -> toXML(l))
                .saveAsTextFile(getOutPath() + "/4_xml");

        return this;
    }

    private String toXML(LinkedEntity l) {

        return null;
    }

    public SparkSession getSpark() {
        return spark;
    }

    public GraphJoiner setSpark(SparkSession spark) {
        this.spark = spark;
        return this;
    }

    public String getInputPath() {
        return inputPath;
    }

    public GraphJoiner setInputPath(String inputPath) {
        this.inputPath = inputPath;
        return this;
    }

    public String getOutPath() {
        return outPath;
    }

    public GraphJoiner setOutPath(String outPath) {
        this.outPath = outPath;
        return this;
    }

    // HELPERS

    private OafEntity parseOaf(final String json, final String type) {
        final ObjectMapper o = new ObjectMapper();
        try {
            switch (type) {
                case "publication":
                    return o.readValue(json, Publication.class);
                case "dataset":
                    return o.readValue(json, Dataset.class);
                case "otherresearchproduct":
                    return o.readValue(json, OtherResearchProduct.class);
                case "software":
                    return o.readValue(json, Software.class);
                case "datasource":
                    return o.readValue(json, Datasource.class);
                case "organization":
                    return o.readValue(json, Organization.class);
                case "project":
                    return o.readValue(json, Project.class);
                default:
                    throw new IllegalArgumentException("invalid type: " + type);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Converts the result of grouping <rel, target> pairs and the entities by source id to LinkedEntity
     * @param p
     * @return
     */
    private LinkedEntity toLinkedEntity(Tuple2<String, Iterable<EntityRelEntity>> p) {
        final LinkedEntity e = new LinkedEntity();
        final List<Tuple> links = Lists.newArrayList();
        for(EntityRelEntity rel : p._2()) {
            if (rel.hasMainEntity() & e.getEntity() == null) {
                e.setEntity(rel.getSource());
            }
            if (rel.hasRelatedEntity()) {
                links.add(new Tuple()
                        .setRelation(rel.getRelation())
                        .setTarget(rel.getTarget()));
            }
        }
        e.setLinks(links);
        if (e.getEntity() == null) {
            throw new IllegalStateException("missing main entity on '" + p._1() + "'");
        }
        return e;
    }

    /**
     * Converts a LinkedEntity to a JoinedEntity
     * @param l
     * @return
     */
    private JoinedEntity toJoinedEntity(LinkedEntity l) {
        return new JoinedEntity().setType(l.getEntity().getType())
                .setEntity(parseOaf(l.getEntity().getOaf(), l.getEntity().getType()))
                .setLinks(l.getLinks()
                        .stream()
                        .map(t -> {
                            final ObjectMapper o = new ObjectMapper();
                            try {
                                return new Tuple2<>(
                                        o.readValue(t.getRelation().getOaf(), Relation.class),
                                        o.readValue(t.getTarget().getOaf(), RelatedEntity.class));
                            } catch (IOException e) {
                                throw new IllegalArgumentException(e);
                            }
                        }).collect(Collectors.toList()));
    }

    /**
     * Reads a set of eu.dnetlib.dhp.schema.oaf.OafEntity objects from a sequence file <className, entity json serialization>,
     * extracts necessary information using json path, wraps the oaf object in a eu.dnetlib.dhp.graph.TypedRow
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
     * extracts necessary information using json path, wraps the oaf object in a eu.dnetlib.dhp.graph.TypedRow
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
                            .setOaf(s);
                });
    }

}
