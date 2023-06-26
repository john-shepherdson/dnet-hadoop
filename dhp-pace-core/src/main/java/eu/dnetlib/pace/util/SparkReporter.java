
package eu.dnetlib.pace.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.util.LongAccumulator;

import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.util.Reporter;
import scala.Serializable;
import scala.Tuple2;

public class SparkReporter implements Serializable, Reporter {

	private final List<Tuple2<String, String>> relations = new ArrayList<>();

	private final Map<String, LongAccumulator> accumulators;

	public SparkReporter(Map<String, LongAccumulator> accumulators) {
		this.accumulators = accumulators;
	}

	public void incrementCounter(
		String counterGroup,
		String counterName,
		long delta,
		Map<String, LongAccumulator> accumulators) {

		final String accumulatorName = String.format("%s::%s", counterGroup, counterName);
		if (accumulators.containsKey(accumulatorName)) {
			accumulators.get(accumulatorName).add(delta);
		}
	}

	@Override
	public void incrementCounter(String counterGroup, String counterName, long delta) {

		incrementCounter(counterGroup, counterName, delta, accumulators);
	}

	@Override
	public void emit(String type, String from, String to) {
		relations.add(new Tuple2<>(from, to));
	}

	public List<Tuple2<String, String>> getRelations() {
		return relations;
	}

	public static Map<String, LongAccumulator> constructAccumulator(
		final DedupConfig dedupConf, final SparkContext context) {

		Map<String, LongAccumulator> accumulators = new HashMap<>();

		String acc1 = String.format("%s::%s", dedupConf.getWf().getEntityType(), "records per hash key = 1");
		accumulators.put(acc1, context.longAccumulator(acc1));
		String acc2 = String
			.format(
				"%s::%s",
				dedupConf.getWf().getEntityType(), "missing " + dedupConf.getWf().getOrderField());
		accumulators.put(acc2, context.longAccumulator(acc2));
		String acc3 = String
			.format(
				"%s::%s",
				dedupConf.getWf().getEntityType(),
				String
					.format(
						"Skipped records for count(%s) >= %s",
						dedupConf.getWf().getOrderField(), dedupConf.getWf().getGroupMaxSize()));
		accumulators.put(acc3, context.longAccumulator(acc3));
		String acc4 = String.format("%s::%s", dedupConf.getWf().getEntityType(), "skip list");
		accumulators.put(acc4, context.longAccumulator(acc4));
		String acc5 = String.format("%s::%s", dedupConf.getWf().getEntityType(), "dedupSimilarity (x2)");
		accumulators.put(acc5, context.longAccumulator(acc5));
		String acc6 = String
			.format(
				"%s::%s", dedupConf.getWf().getEntityType(), "d < " + dedupConf.getWf().getThreshold());
		accumulators.put(acc6, context.longAccumulator(acc6));

		return accumulators;
	}
}
