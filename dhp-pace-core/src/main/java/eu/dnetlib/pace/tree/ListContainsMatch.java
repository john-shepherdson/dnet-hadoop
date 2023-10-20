
package eu.dnetlib.pace.tree;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractListComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

/**
 * The Class Contains match
 *
 * @author miconis
 * */
@ComparatorClass("listContainsMatch")
public class ListContainsMatch extends AbstractListComparator {

	private Map<String, String> params;
	private boolean CASE_SENSITIVE;
	private String STRING;
	private String AGGREGATOR;

	public ListContainsMatch(Map<String, String> params) {
		super(params);
		this.params = params;

		// read parameters
		CASE_SENSITIVE = Boolean.parseBoolean(params.getOrDefault("caseSensitive", "false"));
		STRING = params.get("string");
		AGGREGATOR = params.get("bool");
	}

	@Override
	public double compare(List<String> sa, List<String> sb, Config conf) {
		if (sa.isEmpty() || sb.isEmpty()) {
			return -1;
		}

		if (!CASE_SENSITIVE) {
			sa = sa.stream().map(String::toLowerCase).collect(Collectors.toList());
			sb = sb.stream().map(String::toLowerCase).collect(Collectors.toList());
			STRING = STRING.toLowerCase();
		}

		switch (AGGREGATOR) {
			case "AND":
				if (sa.contains(STRING) && sb.contains(STRING))
					return 1.0;
				break;
			case "OR":
				if (sa.contains(STRING) || sb.contains(STRING))
					return 1.0;
				break;
			case "XOR":
				if (sa.contains(STRING) ^ sb.contains(STRING))
					return 1.0;
				break;
			default:
				return 0.0;
		}
		return 0.0;

	}
}
