
package eu.dnetlib.pace.clustering;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.pace.config.Config;

@ClusteringClass("legalnameclustering")
public class LegalnameClustering extends AbstractClusteringFunction {

	private static final Pattern CITY_CODE_PATTERN = Pattern.compile("city::\\d+");
	private static final Pattern KEYWORD_CODE_PATTERN = Pattern.compile("key::\\d+");

	public LegalnameClustering(Map<String, Object> params) {
		super(params);
	}

	public Set<String> getRegexList(String input, Pattern codeRegex) {
		Matcher matcher = codeRegex.matcher(input);
		Set<String> cities = new HashSet<>();
		while (matcher.find()) {
			cities.add(matcher.group());
		}
		return cities;
	}

	@Override
	protected Collection<String> doApply(final Config conf, String s) {

		// list of combination to return as result
		final Collection<String> combinations = new LinkedHashSet<String>();

		for (String keyword : getRegexList(s, KEYWORD_CODE_PATTERN)) {
			for (String city : getRegexList(s, CITY_CODE_PATTERN)) {
				combinations.add(keyword + "-" + city);
				if (combinations.size() >= paramOrDefault("max", 2)) {
					return combinations;
				}
			}
		}

		return combinations;
	}

	@Override
	public Collection<String> apply(final Config conf, List<String> fields) {
		return fields
			.stream()
			.filter(f -> !f.isEmpty())
			.map(s -> doApply(conf, s))
			.map(c -> filterBlacklisted(c, ngramBlacklist))
			.flatMap(c -> c.stream())
			.filter(StringUtils::isNotBlank)
			.collect(Collectors.toCollection(HashSet::new));
	}
}
