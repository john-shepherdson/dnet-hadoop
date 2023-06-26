
package eu.dnetlib.pace.tree.support;

import java.util.Map;

import com.wcohen.ss.AbstractStringDistance;

import eu.dnetlib.pace.config.Config;

public abstract class AbstractStringComparator extends AbstractComparator<String> {
	protected AbstractStringComparator(Map<String, String> params) {
		super(params);
	}

	protected AbstractStringComparator(Map<String, String> params, AbstractStringDistance ssalgo) {
		super(params, ssalgo);
	}

	protected AbstractStringComparator(double weight, AbstractStringDistance ssalgo) {
		super(weight, ssalgo);
	}

	protected AbstractStringComparator(AbstractStringDistance ssalgo) {
		super(ssalgo);
	}

	public double distance(final String a, final String b, final Config conf) {
		if (a.isEmpty() || b.isEmpty()) {
			return -1; // return -1 if a field is missing
		}
		double score = ssalgo.score(a, b);
		return normalize(score);
	}

	@Override
	public double compare(Object a, Object b, Config conf) {
		return compare(toString(a), toString(b), conf);
	}

	public double compare(final String a, final String b, final Config conf) {
		if (a.isEmpty() || b.isEmpty())
			return -1;
		return distance(a, b, conf);
	}

}
