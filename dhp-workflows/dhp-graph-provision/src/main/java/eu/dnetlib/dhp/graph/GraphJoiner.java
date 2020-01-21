package eu.dnetlib.dhp.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.jayway.jsonpath.JsonPath;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

public class GraphJoiner implements Serializable {

    public static final int MAX_RELS = 10;

    public void join(final SparkSession spark, final String inputPath, final String hiveDbName, final String outPath) {

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        final String entityIdPath = "$.id";

        JavaPairRDD<String, TypedRow> datasource = readPathEntity(sc, entityIdPath, inputPath, "datasource");
        JavaPairRDD<String, TypedRow> organization = readPathEntity(sc, entityIdPath, inputPath, "organization");
        JavaPairRDD<String, TypedRow> project = readPathEntity(sc, entityIdPath, inputPath, "project");
        JavaPairRDD<String, TypedRow> dataset = readPathEntity(sc, entityIdPath, inputPath, "dataset");
        JavaPairRDD<String, TypedRow> otherresearchproduct = readPathEntity(sc, entityIdPath, inputPath, "otherresearchproduct");
        JavaPairRDD<String, TypedRow> software = readPathEntity(sc, entityIdPath, inputPath, "software");
        JavaPairRDD<String, TypedRow> publication = readPathEntity(sc, entityIdPath, inputPath, "publication");

        final String entitiesPath = outPath + "/entities";
        datasource
                .union(organization)
                .union(project)
                .union(dataset)
                .union(otherresearchproduct)
                .union(software)
                .union(publication)
                .map(e -> new EntityRelEntity().setSource(e._2()))
                .map(e -> new ObjectMapper().writeValueAsString(e))
                .saveAsTextFile(entitiesPath, GzipCodec.class);

        JavaPairRDD<String, EntityRelEntity> entities = sc.textFile(entitiesPath)
                .map(t -> new ObjectMapper().readValue(t, EntityRelEntity.class))
                .mapToPair(t -> new Tuple2<>(t.getSource().getSource(), t));

        final JavaPairRDD<String, EntityRelEntity> relation = readPathRelation(sc, inputPath)
                .map(p -> new EntityRelEntity().setRelation(p))
                .mapToPair(p -> new Tuple2<>(p.getRelation().getSource(), p))
                .groupByKey()
                .map(p -> Iterables.limit(p._2(), MAX_RELS))
                .flatMap(p -> p.iterator())
                .mapToPair(p -> new Tuple2<>(p.getRelation().getTarget(), p));

        final String joinByTargetPath = outPath + "/join_by_target";
        relation.join(entities)
                .map(s -> new EntityRelEntity()
                        .setRelation(s._2()._1().getRelation())
                        .setTarget(s._2()._2().getSource()))
                .map(e -> new ObjectMapper().writeValueAsString(e))
                .saveAsTextFile(joinByTargetPath, GzipCodec.class);


        JavaPairRDD<String, EntityRelEntity> bySource = sc.textFile(joinByTargetPath)
                .map(t -> new ObjectMapper().readValue(t, EntityRelEntity.class))
                .mapToPair(t -> new Tuple2<>(t.getRelation().getSource(), t));

        entities
                .union(bySource)
                .groupByKey()   // by source id
                .map(p -> {
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
                })
                .map(e -> new ObjectMapper().writeValueAsString(e))
                .saveAsTextFile(outPath + "/linked_entities", GzipCodec.class);
     }

    private JavaPairRDD<String, TypedRow> readPathEntity(final JavaSparkContext sc, final String idPath, final String inputPath, final String type) {
        return sc.sequenceFile(inputPath + "/" + type, Text.class, Text.class)
                .mapToPair((PairFunction<Tuple2<Text, Text>, String, TypedRow>) item -> {
                    final String json = item._2().toString();
                    final String id = JsonPath.read(json, idPath);
                    return new Tuple2<>(id, new TypedRow(id, type, json));
                });
    }

    private JavaRDD<TypedRow> readPathRelation(final JavaSparkContext sc, final String inputPath) {
        return sc.sequenceFile(inputPath + "/relation", Text.class, Text.class)
                .map(item -> {
                    final String json = item._2().toString();
                    final String source = JsonPath.read(json, "$.source");
                    final String target = JsonPath.read(json, "$.target");
                    return new TypedRow(source, target, "relation", json);
                });
    }

}
