
package eu.dnetlib.pace.tree;

import java.util.Map;
import java.util.Set;

import com.wcohen.ss.AbstractStringDistance;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractStringComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("jaroWinklerLegalname")
public class JaroWinklerLegalname extends AbstractStringComparator {

	private Map<String, String> params;

	private final String CITY_CODE_REGEX = "city::\\d+";
	private final String KEYWORD_CODE_REGEX = "key::\\d+";

	public JaroWinklerLegalname(Map<String, String> params) {
		super(params, new com.wcohen.ss.JaroWinkler());
		this.params = params;
	}

	public JaroWinklerLegalname(double weight) {
		super(weight, new com.wcohen.ss.JaroWinkler());
	}

	protected JaroWinklerLegalname(double weight, AbstractStringDistance ssalgo) {
		super(weight, ssalgo);
	}

	@Override
	public double distance(String a, String b, final Config conf) {

		String ca = a.replaceAll(CITY_CODE_REGEX, "").replaceAll(KEYWORD_CODE_REGEX, " ");
		String cb = b.replaceAll(CITY_CODE_REGEX, "").replaceAll(KEYWORD_CODE_REGEX, " ");

		ca = ca.replaceAll("[ ]{2,}", " ");
		cb = cb.replaceAll("[ ]{2,}", " ");

		if (ca.isEmpty() && cb.isEmpty())
			return 1.0;
		else
			return normalize(ssalgo.score(ca, cb));
	}

	@Override
	public double getWeight() {
		return super.weight;
	}

	@Override
	protected double normalize(double d) {
		return d;
	}

}
