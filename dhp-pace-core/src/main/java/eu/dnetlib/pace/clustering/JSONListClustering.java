
package eu.dnetlib.pace.clustering;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

import eu.dnetlib.pace.common.AbstractPaceFunctions;
import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.util.MapDocumentUtil;

@ClusteringClass("jsonlistclustering")
public class JSONListClustering extends AbstractPaceFunctions implements ClusteringFunction {

	private Map<String, Object> params;

	public JSONListClustering(Map<String, Object> params) {
		this.params = params;
	}

	@Override
	public Map<String, Object> getParams() {
		return params;
	}

	@Override
	public Collection<String> apply(Config conf, List<String> fields) {
		return fields
			.stream()
			.filter(f -> !f.isEmpty())
			.map(s -> doApply(conf, s))
			.filter(StringUtils::isNotBlank)
			.collect(Collectors.toCollection(HashSet::new));
	}

	private String doApply(Config conf, String json) {
		StringBuilder st = new StringBuilder(); // to build the string used for comparisons basing on the jpath into
		// parameters
		final DocumentContext documentContext = JsonPath
			.using(Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS))
			.parse(json);

		// for each path in the param list
		for (String key : params.keySet().stream().filter(k -> k.contains("jpath")).collect(Collectors.toList())) {
			String path = params.get(key).toString();
			String value = MapDocumentUtil.getJPathString(path, documentContext);
			if (value == null || value.isEmpty())
				value = "";
			st.append(value);
			st.append(" ");
		}

		st.setLength(st.length() - 1);

		if (StringUtils.isBlank(st)) {
			return "1";
		}
		return st.toString();
	}
}
