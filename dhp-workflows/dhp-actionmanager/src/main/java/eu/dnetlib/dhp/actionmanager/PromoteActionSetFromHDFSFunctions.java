package eu.dnetlib.dhp.actionmanager;

import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.expressions.Aggregator;
import scala.Tuple2;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class PromoteActionSetFromHDFSFunctions {

    public static <T extends Oaf> Dataset<T> joinOafEntityWithActionPayloadAndMerge(Dataset<T> oafDS,
                                                                                    Dataset<String> actionPayloadDS,
                                                                                    SerializableSupplier<Function<T, String>> oafIdFn,
                                                                                    SerializableSupplier<BiFunction<String, Class<T>, T>> actionPayloadToOafFn,
                                                                                    SerializableSupplier<BiFunction<T, T, T>> mergeAndGetFn,
                                                                                    Class<T> clazz) {
        Dataset<Tuple2<String, T>> oafWithIdDS = oafDS
                .map((MapFunction<T, Tuple2<String, T>>) value -> new Tuple2<>(oafIdFn.get().apply(value), value),
                        Encoders.tuple(Encoders.STRING(), Encoders.kryo(clazz)));

        Dataset<Tuple2<String, T>> actionPayloadWithIdDS = actionPayloadDS
                .map((MapFunction<String, T>) value -> actionPayloadToOafFn.get().apply(value, clazz), Encoders.kryo(clazz))
                .filter((FilterFunction<T>) Objects::nonNull)
                .map((MapFunction<T, Tuple2<String, T>>) value -> new Tuple2<>(oafIdFn.get().apply(value), value),
                        Encoders.tuple(Encoders.STRING(), Encoders.kryo(clazz)));

        return oafWithIdDS
                .joinWith(actionPayloadWithIdDS, oafWithIdDS.col("_1").equalTo(actionPayloadWithIdDS.col("_1")), "left_outer")
                .map((MapFunction<Tuple2<Tuple2<String, T>, Tuple2<String, T>>, T>) value -> {
                    T left = value._1()._2();
                    return Optional
                            .ofNullable(value._2())
                            .map(Tuple2::_2)
                            .map(x -> mergeAndGetFn.get().apply(left, x))
                            .orElse(left);
                }, Encoders.kryo(clazz));
    }

    public static <T extends Oaf> Dataset<T> groupOafByIdAndMerge(Dataset<T> oafDS,
                                                                  SerializableSupplier<Function<T, String>> oafIdFn,
                                                                  SerializableSupplier<BiFunction<T, T, T>> mergeAndGetFn,
                                                                  Class<T> clazz) {
        return oafDS
                .groupByKey((MapFunction<T, String>) x -> oafIdFn.get().apply(x), Encoders.STRING())
                .reduceGroups((ReduceFunction<T>) (v1, v2) -> mergeAndGetFn.get().apply(v1, v2))
                .map((MapFunction<Tuple2<String, T>, T>) Tuple2::_2, Encoders.kryo(clazz));
    }

    public static <T extends Oaf> Dataset<T> groupOafByIdAndMergeUsingAggregator(Dataset<T> oafDS,
                                                                                 SerializableSupplier<T> zeroFn,
                                                                                 SerializableSupplier<Function<T, String>> idFn,
                                                                                 Class<T> clazz) {
        TypedColumn<T, T> aggregator = new OafAggregator<>(zeroFn, clazz).toColumn();
        return oafDS
                .groupByKey((MapFunction<T, String>) x -> idFn.get().apply(x), Encoders.STRING())
                .agg(aggregator)
                .map((MapFunction<Tuple2<String, T>, T>) Tuple2::_2, Encoders.kryo(clazz));
    }

    public static class OafAggregator<T extends Oaf> extends Aggregator<T, T, T> {
        private SerializableSupplier<T> zero;
        private Class<T> clazz;

        public OafAggregator(SerializableSupplier<T> zero, Class<T> clazz) {
            this.zero = zero;
            this.clazz = clazz;
        }

        @Override
        public T zero() {
            return zero.get();
        }

        @Override
        public T reduce(T b, T a) {
            return mergeFrom(b, a);
        }

        @Override
        public T merge(T b1, T b2) {
            return mergeFrom(b1, b2);
        }

        private T mergeFrom(T left, T right) {
            if (isNonNull(left)) {
                if (left instanceof Relation) {
                    ((Relation) left).mergeFrom((Relation) right);
                    return left;
                }
                ((OafEntity) left).mergeFrom((OafEntity) right);
                return left;
            }

            if (right instanceof Relation) {
                ((Relation) right).mergeFrom((Relation) left);
                return right;
            }
            ((OafEntity) right).mergeFrom((OafEntity) left);
            return right;
        }

        private Boolean isNonNull(T a) {
            return Objects.nonNull(a.getLastupdatetimestamp());
        }

        @Override
        public T finish(T reduction) {
            return reduction;
        }

        @Override
        public Encoder<T> bufferEncoder() {
            return Encoders.kryo(clazz);
        }

        @Override
        public Encoder<T> outputEncoder() {
            return Encoders.kryo(clazz);
        }

    }
}