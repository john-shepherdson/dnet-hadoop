
package eu.dnetlib.pace.tree;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Sets;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractListComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;
import eu.dnetlib.pace.util.MapDocumentUtil;

@ComparatorClass("jsonListMatch")
public class JsonListMatch extends AbstractListComparator {

	private static final Log log = LogFactory.getLog(JsonListMatch.class);
	private Map<String, String> params;

	private String MODE; // "percentage" or "count"

	public JsonListMatch(final Map<String, String> params) {
		super(params);
		this.params = params;

		MODE = params.getOrDefault("mode", "percentage");
	}

	@Override
	public double compare(final List<String> sa, final List<String> sb, final Config conf) {
		if (sa.isEmpty() || sb.isEmpty()) {
			return -1;
		}

		final Set<String> ca = sa.stream().map(this::toComparableString).collect(Collectors.toSet());
		final Set<String> cb = sb.stream().map(this::toComparableString).collect(Collectors.toSet());

		int incommon = Sets.intersection(ca, cb).size();
		int simDiff = Sets.symmetricDifference(ca, cb).size();

		if (incommon + simDiff == 0) {
			return 0.0;
		}

		if (MODE.equals("percentage"))
			return (double) incommon / (incommon + simDiff);
		else
			return incommon;

	}

	// converts every json into a comparable string basing on parameters
	private String toComparableString(String json) {

		StringBuilder st = new StringBuilder(); // to build the string used for comparisons basing on the jpath into
												// parameters
		final DocumentContext documentContext = JsonPath
			.using(Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS))
			.parse(json);
		// for each path in the param list
		for (String key : params.keySet().stream().filter(k -> k.contains("jpath")).collect(Collectors.toList())) {
			String path = params.get(key);
			String value = MapDocumentUtil.getJPathString(path, documentContext);
			if (value == null || value.isEmpty())
				value = "";
			st.append(value);
			st.append("::");
		}

		st.setLength(st.length() - 2);
		return st.toString();
	}
}
