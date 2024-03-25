
package eu.dnetlib.dhp.actionmanager.promote;

import static eu.dnetlib.dhp.schema.common.ModelSupport.isSubClass;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.expressions.Aggregator;

import eu.dnetlib.dhp.common.FunctionalInterfaceSupport.SerializableSupplier;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import scala.Tuple2;

/** Promote action payload functions. */
public class PromoteActionPayloadFunctions {

	private PromoteActionPayloadFunctions() {
	}

	/**
	 * Joins dataset representing graph table with dataset representing action payload using supplied functions.
	 *
	 * @param rowDS Dataset representing graph table
	 * @param actionPayloadDS Dataset representing action payload
	 * @param rowIdFn Function used to get the id of graph table row
	 * @param actionPayloadIdFn Function used to get id of action payload instance
	 * @param mergeAndGetFn Function used to merge graph table row and action payload instance
	 * @param promoteActionStrategy the Actionset promotion strategy
	 * @param rowClazz Class of graph table
	 * @param actionPayloadClazz Class of action payload
	 * @param <G> Type of graph table row
	 * @param <A> Type of action payload instance
	 * @return Dataset of merged graph table rows and action payload instances
	 */
	public static <G extends Oaf, A extends Oaf> Dataset<G> joinGraphTableWithActionPayloadAndMerge(
		Dataset<G> rowDS,
		Dataset<A> actionPayloadDS,
		SerializableSupplier<Function<G, String>> rowIdFn,
		SerializableSupplier<Function<A, String>> actionPayloadIdFn,
		SerializableSupplier<BiFunction<G, A, G>> mergeAndGetFn,
		PromoteAction.Strategy promoteActionStrategy,
		Class<G> rowClazz,
		Class<A> actionPayloadClazz) {
		if (!isSubClass(rowClazz, actionPayloadClazz)) {
			throw new RuntimeException(
				"action payload type must be the same or be a super type of table row type");
		}

		Dataset<Tuple2<String, G>> rowWithIdDS = mapToTupleWithId(rowDS, rowIdFn, rowClazz);
		Dataset<Tuple2<String, A>> actionPayloadWithIdDS = mapToTupleWithId(
			actionPayloadDS, actionPayloadIdFn, actionPayloadClazz);

		return rowWithIdDS
			.joinWith(
				actionPayloadWithIdDS,
				rowWithIdDS.col("_1").equalTo(actionPayloadWithIdDS.col("_1")),
				PromoteAction.joinTypeForStrategy(promoteActionStrategy))
			.map(
				(MapFunction<Tuple2<Tuple2<String, G>, Tuple2<String, A>>, G>) value -> {
					Optional<G> rowOpt = Optional.ofNullable(value._1()).map(Tuple2::_2);
					Optional<A> actionPayloadOpt = Optional.ofNullable(value._2()).map(Tuple2::_2);
					return rowOpt
						.map(
							row -> actionPayloadOpt
								.map(
									actionPayload -> mergeAndGetFn.get().apply(row, actionPayload))
								.orElse(row))
						.orElseGet(
							() -> actionPayloadOpt
								.filter(
									actionPayload -> actionPayload.getClass().equals(rowClazz))
								.map(rowClazz::cast)
								.orElse(null));
				},
				Encoders.kryo(rowClazz))
			.filter((FilterFunction<G>) Objects::nonNull);
	}

	private static <T extends Oaf> Dataset<Tuple2<String, T>> mapToTupleWithId(
		Dataset<T> ds, SerializableSupplier<Function<T, String>> idFn, Class<T> clazz) {
		return ds
			.map(
				(MapFunction<T, Tuple2<String, T>>) value -> new Tuple2<>(idFn.get().apply(value), value),
				Encoders.tuple(Encoders.STRING(), Encoders.kryo(clazz)));
	}

	/**
	 * Groups graph table by id and aggregates using supplied functions.
	 *
	 * @param rowDS Dataset representing graph table
	 * @param rowIdFn Function used to get the id of graph table row
	 * @param mergeAndGetFn Function used to merge graph table rows
	 * @param zeroFn Function to create a zero/empty instance of graph table row
	 * @param isNotZeroFn Function to check if graph table row is not zero/empty
	 * @param rowClazz Class of graph table
	 * @param <G> Type of graph table row
	 * @return Dataset of aggregated graph table rows
	 */
	public static <G extends Oaf> Dataset<G> groupGraphTableByIdAndMerge(
		Dataset<G> rowDS,
		SerializableSupplier<Function<G, String>> rowIdFn,
		SerializableSupplier<BiFunction<G, G, G>> mergeAndGetFn,
		SerializableSupplier<G> zeroFn,
		SerializableSupplier<Function<G, Boolean>> isNotZeroFn,
		Class<G> rowClazz) {
		TypedColumn<G, G> aggregator = new TableAggregator<>(zeroFn, mergeAndGetFn, isNotZeroFn, rowClazz).toColumn();
		return rowDS
			.filter((FilterFunction<G>) o -> isNotZeroFn.get().apply(o))
			.groupByKey((MapFunction<G, String>) x -> rowIdFn.get().apply(x), Encoders.STRING())
			.agg(aggregator)
			.map((MapFunction<Tuple2<String, G>, G>) Tuple2::_2, Encoders.kryo(rowClazz));
	}

	/**
	 * Aggregator to be used for aggregating graph table rows during grouping.
	 *
	 * @param <G> Type of graph table row
	 */
	public static class TableAggregator<G extends Oaf> extends Aggregator<G, G, G> {
		private final SerializableSupplier<G> zeroFn;
		private final SerializableSupplier<BiFunction<G, G, G>> mergeAndGetFn;
		private final SerializableSupplier<Function<G, Boolean>> isNotZeroFn;
		private final Class<G> rowClazz;

		public TableAggregator(
			SerializableSupplier<G> zeroFn,
			SerializableSupplier<BiFunction<G, G, G>> mergeAndGetFn,
			SerializableSupplier<Function<G, Boolean>> isNotZeroFn,
			Class<G> rowClazz) {
			this.zeroFn = zeroFn;
			this.mergeAndGetFn = mergeAndGetFn;
			this.isNotZeroFn = isNotZeroFn;
			this.rowClazz = rowClazz;
		}

		@Override
		public G zero() {
			return zeroFn.get();
		}

		@Override
		public G reduce(G b, G a) {
			return zeroSafeMergeAndGet(b, a);
		}

		@Override
		public G merge(G b1, G b2) {
			return zeroSafeMergeAndGet(b1, b2);
		}

		private G zeroSafeMergeAndGet(G left, G right) {
			Function<G, Boolean> isNotZero = isNotZeroFn.get();
			if (isNotZero.apply(left) && isNotZero.apply(right)) {
				return mergeAndGetFn.get().apply(left, right);
			} else if (isNotZero.apply(left) && !isNotZero.apply(right)) {
				return left;
			} else if (!isNotZero.apply(left) && isNotZero.apply(right)) {
				return right;
			}
			throw new RuntimeException("internal aggregation error: left and right objects are zero");
		}

		@Override
		public G finish(G reduction) {
			return reduction;
		}

		@Override
		public Encoder<G> bufferEncoder() {
			return Encoders.kryo(rowClazz);
		}

		@Override
		public Encoder<G> outputEncoder() {
			return Encoders.kryo(rowClazz);
		}
	}
}
