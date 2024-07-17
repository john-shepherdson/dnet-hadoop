
package eu.dnetlib.pace.tree;

import java.util.Map;
import java.util.Set;

import com.wcohen.ss.AbstractStringDistance;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractStringComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("countryMatch")
public class CountryMatch extends AbstractStringComparator {

	private Map<String, String> params;

	public CountryMatch(Map<String, String> params) {
		super(params, new com.wcohen.ss.JaroWinkler());
		this.params = params;
	}

	public CountryMatch(final double weight) {
		super(weight, new com.wcohen.ss.JaroWinkler());
	}

	protected CountryMatch(final double weight, final AbstractStringDistance ssalgo) {
		super(weight, ssalgo);
	}

	@Override
	public double distance(final String a, final String b, final Config conf) {

		if (a.isEmpty() || b.isEmpty()) {
			return -1.0; // return -1 if a field is missing
		}
		if (a.equalsIgnoreCase("unknown") || b.equalsIgnoreCase("unknown")) {
			return -1.0; // return -1 if a country is UNKNOWN
		}

		return a.equals(b) ? 1.0 : 0;
	}

	@Override
	public double getWeight() {
		return super.weight;
	}

	@Override
	protected double normalize(final double d) {
		return d;
	}

}
