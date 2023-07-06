
package eu.dnetlib.pace.clustering;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.model.Document;
import eu.dnetlib.pace.model.Field;
import eu.dnetlib.pace.model.FieldListImpl;
import eu.dnetlib.pace.model.MapDocument;

public class BlacklistAwareClusteringCombiner extends ClusteringCombiner {

	public static Collection<String> filterAndCombine(final MapDocument a, final Config conf) {
		Document filtered = filter(a, conf.blacklists());
		return combine(filtered, conf);
	}

	private static MapDocument filter(final MapDocument a, final Map<String, List<Pattern>> blacklists) {
		if (blacklists == null || blacklists.isEmpty()) {
			return a;
		}

		final Map<String, Field> filtered = Maps.newHashMap(a.getFieldMap());

		for (final Entry<String, List<Pattern>> e : blacklists.entrySet()) {
			Field fields = a.getFieldMap().get(e.getKey());
			if (fields != null) {
				final FieldListImpl fl = new FieldListImpl();

				for (Field f : fields) {
					if (!isBlackListed(f.stringValue(), e.getValue())) {
						fl.add(f);
					}
				}

				filtered.put(e.getKey(), fl);
			}
		}

		return new MapDocument(a.getIdentifier(), filtered);
	}

	private static boolean isBlackListed(String value, List<Pattern> blacklist) {
		for (Pattern pattern : blacklist) {
			if (pattern.matcher(value).matches()) {
				return true;
			}
		}

		return false;
	}

}
