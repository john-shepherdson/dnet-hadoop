package eu.dnetlib.dhp.oa.dedup;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.pace.config.DedupConfig;
import java.util.Collection;
import java.util.Iterator;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class DedupRecordFactory {

    protected static final ObjectMapper OBJECT_MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static <T extends OafEntity> Dataset<T> createDedupRecord(
            final SparkSession spark,
            final String mergeRelsInputPath,
            final String entitiesInputPath,
            final Class<T> clazz,
            final DedupConfig dedupConf) {

        long ts = System.currentTimeMillis();

        // <id, json_entity>
        Dataset<Tuple2<String, T>> entities =
                spark.read()
                        .textFile(entitiesInputPath)
                        .map(
                                (MapFunction<String, Tuple2<String, T>>)
                                        it -> {
                                            T entity = OBJECT_MAPPER.readValue(it, clazz);
                                            return new Tuple2<>(entity.getId(), entity);
                                        },
                                Encoders.tuple(Encoders.STRING(), Encoders.kryo(clazz)));

        // <source, target>: source is the dedup_id, target is the id of the mergedIn
        Dataset<Tuple2<String, String>> mergeRels =
                spark.read()
                        .load(mergeRelsInputPath)
                        .as(Encoders.bean(Relation.class))
                        .where("relClass == 'merges'")
                        .map(
                                (MapFunction<Relation, Tuple2<String, String>>)
                                        r -> new Tuple2<>(r.getSource(), r.getTarget()),
                                Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

        // <dedup_id, json_entity_merged>
        return mergeRels
                .joinWith(entities, mergeRels.col("_1").equalTo(entities.col("_1")), "left_outer")
                .filter(
                        (FilterFunction<Tuple2<Tuple2<String, String>, Tuple2<String, T>>>)
                                value -> value._2() != null)
                .map(
                        (MapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, T>>, T>)
                                value -> value._2()._2(),
                        Encoders.kryo(clazz))
                .groupByKey((MapFunction<T, String>) value -> value.getId(), Encoders.STRING())
                .mapGroups(
                        (MapGroupsFunction<String, T, T>)
                                (key, values) -> entityMerger(key, values, ts, clazz),
                        Encoders.bean(clazz));
    }

    private static <T extends OafEntity> T entityMerger(
            String id, Iterator<T> entities, final long ts, Class<T> clazz) {
        try {
            T entity = clazz.newInstance();
            entity.setId(id);
            if (entity.getDataInfo() == null) {
                entity.setDataInfo(new DataInfo());
            }
            entity.getDataInfo().setTrust("0.9");
            entity.setLastupdatetimestamp(ts);

            final Collection<String> dates = Lists.newArrayList();
            entities.forEachRemaining(
                    e -> {
                        entity.mergeFrom(e);
                        if (ModelSupport.isSubClass(e, Result.class)) {
                            Result r1 = (Result) e;
                            Result er = (Result) entity;
                            er.setAuthor(DedupUtility.mergeAuthor(er.getAuthor(), r1.getAuthor()));

                            if (er.getDateofacceptance() != null) {
                                dates.add(r1.getDateofacceptance().getValue());
                            }
                        }
                    });

            if (ModelSupport.isSubClass(entity, Result.class)) {
                ((Result) entity).setDateofacceptance(DatePicker.pick(dates));
            }
            return entity;
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }
}
