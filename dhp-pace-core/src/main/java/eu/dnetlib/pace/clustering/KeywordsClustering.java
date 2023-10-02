
package eu.dnetlib.pace.clustering;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.pace.config.Config;

@ClusteringClass("keywordsclustering")
public class KeywordsClustering extends AbstractClusteringFunction {

	public KeywordsClustering(Map<String, Object> params) {
		super(params);
	}

	@Override
	protected Collection<String> doApply(final Config conf, String s) {

		// takes city codes and keywords codes without duplicates
		Set<String> keywords = getKeywords(s, conf.translationMap(), paramOrDefault("windowSize", 4));
		Set<String> cities = getCities(s, paramOrDefault("windowSize", 4));

		// list of combination to return as result
		final Collection<String> combinations = new LinkedHashSet<String>();

		for (String keyword : keywordsToCodes(keywords, conf.translationMap())) {
			for (String city : citiesToCodes(cities)) {
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
			.map(KeywordsClustering::cleanup)
			.map(KeywordsClustering::normalize)
			.map(s -> filterAllStopWords(s))
			.map(s -> doApply(conf, s))
			.map(c -> filterBlacklisted(c, ngramBlacklist))
			.flatMap(c -> c.stream())
			.filter(StringUtils::isNotBlank)
			.collect(Collectors.toCollection(HashSet::new));
	}
}
