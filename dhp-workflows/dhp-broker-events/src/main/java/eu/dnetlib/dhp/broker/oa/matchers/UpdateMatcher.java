
package eu.dnetlib.dhp.broker.oa.matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.pace.config.DedupConfig;

public abstract class UpdateMatcher<T> {

	private final boolean multipleUpdate;
	private final Function<T, Topic> topicFunction;
	private final BiConsumer<OaBrokerMainEntity, T> compileHighlightFunction;
	private final Function<T, String> highlightToStringFunction;

	public UpdateMatcher(final boolean multipleUpdate, final Function<T, Topic> topicFunction,
		final BiConsumer<OaBrokerMainEntity, T> compileHighlightFunction,
		final Function<T, String> highlightToStringFunction) {
		this.multipleUpdate = multipleUpdate;
		this.topicFunction = topicFunction;
		this.compileHighlightFunction = compileHighlightFunction;
		this.highlightToStringFunction = highlightToStringFunction;
	}

	public Collection<UpdateInfo<T>> searchUpdatesForRecord(final OaBrokerMainEntity res,
		final Collection<OaBrokerMainEntity> others,
		final DedupConfig dedupConfig) {

		final Map<String, UpdateInfo<T>> infoMap = new HashMap<>();

		for (final OaBrokerMainEntity source : others) {
			if (source != res) {
				for (final T hl : findDifferences(source, res)) {
					final Topic topic = getTopicFunction().apply(hl);
					if (topic != null) {
						final UpdateInfo<T> info = new UpdateInfo<>(topic, hl, source, res,
							getCompileHighlightFunction(),
							getHighlightToStringFunction(), dedupConfig);

						final String s = DigestUtils.md5Hex(info.getHighlightValueAsString());
						if (!infoMap.containsKey(s) || infoMap.get(s).getTrust() < info.getTrust()) {
							infoMap.put(s, info);
						}
					}
				}
			}
		}

		final Collection<UpdateInfo<T>> values = infoMap.values();

		if (values.isEmpty() || multipleUpdate) {
			return values;
		} else {
			final UpdateInfo<T> v = values
				.stream()
				.sorted((o1, o2) -> Float.compare(o1.getTrust(), o2.getTrust()))
				.findFirst()
				.get();
			return Arrays.asList(v);
		}
	}

	protected abstract List<T> findDifferences(OaBrokerMainEntity source, OaBrokerMainEntity target);

	protected static boolean isMissing(final List<String> list) {
		return list == null || list.isEmpty() || StringUtils.isBlank(list.get(0));
	}

	protected boolean isMissing(final String field) {
		return StringUtils.isBlank(field);
	}

	public boolean isMultipleUpdate() {
		return multipleUpdate;
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

}
