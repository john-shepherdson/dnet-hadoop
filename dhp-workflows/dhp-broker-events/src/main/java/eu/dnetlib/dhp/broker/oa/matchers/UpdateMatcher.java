
package eu.dnetlib.dhp.broker.oa.matchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.util.LongAccumulator;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;

public abstract class UpdateMatcher<T> {

	private final int maxNumber;
	private final Function<T, Topic> topicFunction;
	private final BiConsumer<OaBrokerMainEntity, T> compileHighlightFunction;
	private final Function<T, String> highlightToStringFunction;

	public UpdateMatcher(final int maxNumber, final Function<T, Topic> topicFunction,
		final BiConsumer<OaBrokerMainEntity, T> compileHighlightFunction,
		final Function<T, String> highlightToStringFunction) {
		this.maxNumber = maxNumber;
		this.topicFunction = topicFunction;
		this.compileHighlightFunction = compileHighlightFunction;
		this.highlightToStringFunction = highlightToStringFunction;
	}

	public Collection<UpdateInfo<T>> searchUpdatesForRecord(final OaBrokerMainEntity res,
		final Collection<OaBrokerMainEntity> others,
		final Map<String, LongAccumulator> accumulators) {

		final Map<String, UpdateInfo<T>> infoMap = new HashMap<>();

		for (final OaBrokerMainEntity source : others) {
			if (source != res) {
				for (final T hl : findDifferences(source, res)) {
					final Topic topic = getTopicFunction().apply(hl);
					if (topic != null) {
						final UpdateInfo<T> info = new UpdateInfo<>(topic, hl, source, res,
							getCompileHighlightFunction(),
							getHighlightToStringFunction());

						final String s = DigestUtils.md5Hex(info.getHighlightValueAsString());
						if (!infoMap.containsKey(s) || infoMap.get(s).getTrust() < info.getTrust()) {
							infoMap.put(s, info);
						}
					}
				}
			}
		}

		final List<UpdateInfo<T>> values = infoMap
			.values()
			.stream()
			.sorted((o1, o2) -> Float.compare(o2.getTrust(), o1.getTrust())) // DESCENDING
			.collect(Collectors.toList());

		if (values.isEmpty()) {
			return new ArrayList<>();
		} else if (values.size() > maxNumber) {
			incrementAccumulator(accumulators, maxNumber);
			return values.subList(0, maxNumber);
		} else {
			incrementAccumulator(accumulators, values.size());
			return values;
		}
	}

	protected abstract List<T> findDifferences(OaBrokerMainEntity source, OaBrokerMainEntity target);

	protected static boolean isMissing(final List<String> list) {
		return list == null || list.isEmpty() || StringUtils.isBlank(list.get(0));
	}

	protected boolean isMissing(final String s) {
		return StringUtils.isBlank(s);
	}

	public int getMaxNumber() {
		return maxNumber;
	}

	public Function<T, Topic> getTopicFunction() {
		return topicFunction;
	}

	public BiConsumer<OaBrokerMainEntity, T> getCompileHighlightFunction() {
		return compileHighlightFunction;
	}

	public Function<T, String> getHighlightToStringFunction() {
		return highlightToStringFunction;
	}

	public String accumulatorName() {
		return "event_matcher_" + getClass().getSimpleName().toLowerCase();
	}

	public void incrementAccumulator(final Map<String, LongAccumulator> accumulators, final long n) {
		if (accumulators != null && accumulators.containsKey(accumulatorName())) {
			accumulators.get(accumulatorName()).add(n);
		}
	}

}
