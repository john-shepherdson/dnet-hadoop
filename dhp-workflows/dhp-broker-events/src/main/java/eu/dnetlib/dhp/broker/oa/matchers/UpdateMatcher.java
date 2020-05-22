
package eu.dnetlib.dhp.broker.oa.matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Field;

public abstract class UpdateMatcher<K, T> {

	private final boolean multipleUpdate;

	public UpdateMatcher(final boolean multipleUpdate) {
		this.multipleUpdate = multipleUpdate;
	}

	public Collection<UpdateInfo<T>> searchUpdatesForRecord(final K res, final Collection<K> others) {

		final Map<String, UpdateInfo<T>> infoMap = new HashMap<>();

		for (final K source : others) {
			if (source != res) {
				for (final UpdateInfo<T> info : findUpdates(source, res)) {
					final String s = DigestUtils.md5Hex(info.getHighlightValueAsString());
					if (!infoMap.containsKey(s) || infoMap.get(s).getTrust() < info.getTrust()) {
					} else {
						infoMap.put(s, info);
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

	protected abstract List<UpdateInfo<T>> findUpdates(K source, K target);

	protected abstract UpdateInfo<T> generateUpdateInfo(final T highlightValue,
		final K source,
		final K target);

	protected static boolean isMissing(final List<Field<String>> list) {
		return list == null || list.isEmpty() || StringUtils.isBlank(list.get(0).getValue());
	}

	protected boolean isMissing(final Field<String> field) {
		return field == null || StringUtils.isBlank(field.getValue());
	}

}
