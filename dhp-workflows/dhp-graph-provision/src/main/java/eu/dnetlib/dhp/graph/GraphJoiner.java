package eu.dnetlib.dhp.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;

public class GraphJoiner implements Serializable {

    public static final int MAX_RELS = 100;

    public void join(final SparkSession spark, final String inputPath, final String hiveDbName, final String outPath) {

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        /*
        JavaPairRDD<String, TypedRow> entities = sc.sequenceFile(inputPath + "/publication", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Publication.class))
                .map(oaf -> new TypedRow("publication", oaf))
                .mapToPair(toPair());

         */

        JavaPairRDD<String, TypedRow> entities = sc.sequenceFile(inputPath + "/datasource", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Datasource.class))
                .map(oaf -> new TypedRow("datasource", oaf))
                .mapToPair(toPair())
            .union(sc.sequenceFile(inputPath + "/organization", Text.class, Text.class)
                    .map(item -> new ObjectMapper().readValue(item._2().toString(), Organization.class))
                    .map(oaf -> new TypedRow("organization", oaf))
                    .mapToPair(toPair()))
            .union(sc.sequenceFile(inputPath + "/project", Text.class, Text.class)
                    .map(item -> new ObjectMapper().readValue(item._2().toString(), Project.class))
                    .map(oaf -> new TypedRow("project", oaf))
                    .mapToPair(toPair()))
            .union(sc.sequenceFile(inputPath + "/dataset", Text.class, Text.class)
                    .map(item -> new ObjectMapper().readValue(item._2().toString(), Dataset.class))
                    .map(oaf -> new TypedRow("dataset", oaf))
                    .mapToPair(toPair()))
            .union(sc.sequenceFile(inputPath + "/otherresearchproduct", Text.class, Text.class)
                    .map(item -> new ObjectMapper().readValue(item._2().toString(), OtherResearchProduct.class))
                    .map(oaf -> new TypedRow("otherresearchproduct", oaf))
                    .mapToPair(toPair()))
            .union(sc.sequenceFile(inputPath + "/software", Text.class, Text.class)
                    .map(item -> new ObjectMapper().readValue(item._2().toString(), Software.class))
                    .map(oaf -> new TypedRow("software", oaf))
                    .mapToPair(toPair()));
                /*
            .union(sc.sequenceFile(inputPath + "/publication", Text.class, Text.class)
                    .map(item -> new ObjectMapper().readValue(item._2().toString(), Publication.class))
                    .map(oaf -> new TypedRow("publication", oaf))
                    .mapToPair(toPair()));

         */

        /*
        JavaRDD<Relation> rels = sc.sequenceFile(inputPath + "/relation", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Relation.class))
                .map(oaf -> new TypedRow("relation", oaf))
                .mapToPair(toPair())
                .groupByKey()
                .map(t -> Iterables.limit(t._2(), MAX_RELS))
                .flatMap(t -> t.iterator())
                .map(t -> (Relation) t.getOaf());

        spark.createDataset(rels.rdd(), Encoders.bean(Relation.class))
                .write()
                .mode(SaveMode.Overwrite)
                .saveAsTable(hiveDbName + ".relation_100");
        */

        JavaPairRDD<String, TypedRow> bounded_rels = spark.table(hiveDbName + ".relation_" + MAX_RELS)
                .as(Encoders.bean(Relation.class))
                .javaRDD()
                .map(r -> new TypedRow("relation", r))
                .mapToPair(toPair());

        // build the adjacency list: e -> r
        JavaPairRDD<String, Tuple2<TypedRow, Optional<TypedRow>>> adjacency_list = entities.leftOuterJoin(bounded_rels);

        JavaRDD<EntityRelEntity> linked_entities = adjacency_list
                .mapToPair(toPairTarget())          // make rel.targetid explicit so that we can join it
                .leftOuterJoin(entities)            // again with the entities to get the target entity
                .map(l -> toEntityRelEntity(l));    // and map it to a more readable representation

        spark.createDataFrame(linked_entities, EntityRelEntity.class)
                .write()
                .mode(SaveMode.Overwrite)
                .saveAsTable(hiveDbName + ".linked_entities");
     }

    private EntityRelEntity toEntityRelEntity(Tuple2<String, Tuple2<Tuple2<String, Tuple2<TypedRow, Optional<TypedRow>>>, Optional<TypedRow>>> l) {
        // extract the entity source
        final EntityRelEntity res = new EntityRelEntity(l._2()._1()._2()._1());

        if(l._2()._1()._2()._2().isPresent() && l._2()._2().isPresent()) {

            // extract the relationship
            res.setRelation((Relation) l._2()._1()._2()._2().get().getOaf());

            // extract the related entity
            res.setTarget(l._2()._2().get());
        }

        return res;
    }

    private PairFunction<Tuple2<String, Tuple2<TypedRow, Optional<TypedRow>>>, String, Tuple2<String, Tuple2<TypedRow, Optional<TypedRow>>>> toPairTarget() {
        return e -> {
            Optional<TypedRow> o = e._2()._2();
            if (o.isPresent()) {
                return new Tuple2<>(((Relation) o.get().getOaf()).getTarget(), e);
            } else {
                return new Tuple2<>(null, e);
            }
        };
    }

    private PairFunction<TypedRow, String, TypedRow> toPair() {
        return e -> {
            if (!"relation".equals(e.getType())) {
                return new Tuple2<>( ((OafEntity) e.getOaf()).getId(), e);
            } else {
                return new Tuple2<>( ((Relation) e.getOaf()).getSource(), e);
            }
        };
    }

}
