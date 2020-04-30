
package eu.dnetlib.dhp.oa.dedup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.util.LongAccumulator;

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
}
