
package eu.dnetlib.pace.tree;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractStringComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("codeMatch")
public class CodeMatch extends AbstractStringComparator {

	private Map<String, String> params;

	private Pattern CODE_REGEX;

	public CodeMatch(Map<String, String> params) {
		super(params);
		this.params = params;
		this.CODE_REGEX = Pattern.compile(params.getOrDefault("codeRegex", "[a-zA-Z]+::\\d+"));
	}

	public Set<String> getRegexList(String input) {
		Matcher matcher = this.CODE_REGEX.matcher(input);
		Set<String> cities = new HashSet<>();
		while (matcher.find()) {
			cities.add(matcher.group());
		}
		return cities;
	}

	@Override
	public double distance(final String a, final String b, final Config conf) {

		Set<String> codes1 = getRegexList(a);
		Set<String> codes2 = getRegexList(b);

		// if no codes are detected, the comparator gives 1.0
		if (codes1.isEmpty() && codes2.isEmpty())
			return 1.0;
		else {
			if (codes1.isEmpty() ^ codes2.isEmpty())
				return -1; // undefined if one of the two has no codes
			return commonElementsPercentage(codes1, codes2);
		}
	}
}
