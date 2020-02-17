package eu.dnetlib.dhp.actionmanager;

import eu.dnetlib.dhp.schema.oaf.OafEntity;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;

import java.util.Optional;
import java.util.function.BiFunction;

public class PromoteActionSetFromHDFSFunctions {

    public static <T extends OafEntity> Dataset<T> groupEntitiesByIdAndMerge(Dataset<T> entityDS,
                                                                             Class<T> clazz) {
        return entityDS
                .groupByKey((MapFunction<T, String>) OafEntity::getId, Encoders.STRING())
                .reduceGroups((ReduceFunction<T>) (x1, x2) -> {
                    x1.mergeFrom(x2);
                    return x1;
                })
                .map((MapFunction<Tuple2<String, T>, T>) pair -> pair._2, Encoders.bean(clazz));
    }

    public static <T extends OafEntity, S> Dataset<T> joinEntitiesWithActionPayloadAndMerge(Dataset<T> entityDS,
                                                                                            Dataset<S> actionPayloadDS,
                                                                                            BiFunction<Dataset<T>, Dataset<S>, Column> entityToActionPayloadJoinExpr,
                                                                                            BiFunction<S, Class<T>, T> actionPayloadToEntityFn,
                                                                                            Class<T> clazz) {
        return entityDS
                .joinWith(actionPayloadDS, entityToActionPayloadJoinExpr.apply(entityDS, actionPayloadDS), "left_outer")
                .map((MapFunction<Tuple2<T, S>, T>) pair -> Optional
                        .ofNullable(pair._2())
                        .map(x -> {
                            T entity = actionPayloadToEntityFn.apply(x, clazz);
                            pair._1().mergeFrom(entity);
                            return pair._1();
                        })
                        .orElse(pair._1()), Encoders.bean(clazz));
    }


}
