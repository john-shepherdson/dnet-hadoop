
package eu.dnetlib.pace.tree;

import java.util.Map;

import com.wcohen.ss.AbstractStringDistance;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("alwaysMatch")
public class AlwaysMatch<T> extends AbstractComparator<T> {

	public AlwaysMatch(final Map<String, String> params) {
		super(params, new com.wcohen.ss.JaroWinkler());
	}

	public AlwaysMatch(final double weight) {
		super(weight, new com.wcohen.ss.JaroWinkler());
	}

	protected AlwaysMatch(final double weight, final AbstractStringDistance ssalgo) {
		super(weight, ssalgo);
	}

	@Override
	public double compare(final Object a, final Object b, final Config conf) {
		return 1.0;
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
