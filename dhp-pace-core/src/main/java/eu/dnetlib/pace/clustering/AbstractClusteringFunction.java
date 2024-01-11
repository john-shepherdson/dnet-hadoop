
package eu.dnetlib.pace.clustering;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.pace.common.AbstractPaceFunctions;
import eu.dnetlib.pace.config.Config;

public abstract class AbstractClusteringFunction extends AbstractPaceFunctions implements ClusteringFunction {

	protected Map<String, Object> params;

	public AbstractClusteringFunction(final Map<String, Object> params) {
		this.params = params;
	}

	protected abstract Collection<String> doApply(Config conf, String s);

	@Override
	public Collection<String> apply(Config conf, List<String> fields) {
		return fields
			.stream()
			.filter(f -> !f.isEmpty())
			.map(s -> normalize(s))
			.map(s -> filterAllStopWords(s))
			.map(s -> doApply(conf, s))
			.map(c -> filterBlacklisted(c, ngramBlacklist))
			.flatMap(c -> c.stream())
			.filter(StringUtils::isNotBlank)
			.collect(Collectors.toCollection(HashSet::new));
	}

	public Map<String, Object> getParams() {
		return params;
	}

	protected Integer param(String name) {
		Object val = params.get(name);
		if (val == null)
			return null;
		if (val instanceof Number) {
			return ((Number) val).intValue();
		}
		return Integer.parseInt(val.toString());
	}

	protected int paramOrDefault(String name, int i) {
		Integer res = param(name);
		if (res == null)
			res = i;
		return res;
	}
}
