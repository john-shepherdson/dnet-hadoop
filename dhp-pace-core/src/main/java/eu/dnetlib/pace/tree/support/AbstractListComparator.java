
package eu.dnetlib.pace.tree.support;

import java.util.List;
import java.util.Map;

import com.wcohen.ss.AbstractStringDistance;

import eu.dnetlib.pace.config.Config;

abstract public class AbstractListComparator extends AbstractComparator<List<String>> {
	protected AbstractListComparator(Map<String, String> params) {
		super(params);
	}

	protected AbstractListComparator(Map<String, String> params, AbstractStringDistance ssalgo) {
		super(params, ssalgo);
	}

	protected AbstractListComparator(double weight, AbstractStringDistance ssalgo) {
		super(weight, ssalgo);
	}

	protected AbstractListComparator(AbstractStringDistance ssalgo) {
		super(ssalgo);
	}

	@Override
	public double compare(Object a, Object b, Config conf) {
		return compare(toList(a), toList(b), conf);
	}

	public double compare(final List<String> a, final List<String> b, final Config conf) {
		if (a.isEmpty() || b.isEmpty())
			return -1;

		return distance(concat(a), concat(b), conf);
	}
}
