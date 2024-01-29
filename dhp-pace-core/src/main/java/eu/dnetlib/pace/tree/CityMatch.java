
package eu.dnetlib.pace.tree;

import java.util.Map;
import java.util.Set;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractStringComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("cityMatch")
public class CityMatch extends AbstractStringComparator {

	private Map<String, String> params;

	public CityMatch(Map<String, String> params) {
		super(params);
		this.params = params;
	}

	@Override
	public double distance(final String a, final String b, final Config conf) {

		String ca = cleanup(a);
		String cb = cleanup(b);

		ca = normalize(ca);
		cb = normalize(cb);

		ca = filterAllStopWords(ca);
		cb = filterAllStopWords(cb);

		Set<String> cities1 = getCities(ca, Integer.parseInt(params.getOrDefault("windowSize", "4")));
		Set<String> cities2 = getCities(cb, Integer.parseInt(params.getOrDefault("windowSize", "4")));

		Set<String> codes1 = citiesToCodes(cities1);
		Set<String> codes2 = citiesToCodes(cities2);

		// if no cities are detected, the comparator gives 1.0
		if (codes1.isEmpty() && codes2.isEmpty())
			return 1.0;
		else {
			if (codes1.isEmpty() ^ codes2.isEmpty())
				return -1; // undefined if one of the two has no cities
			return commonElementsPercentage(codes1, codes2);
		}
	}
}
