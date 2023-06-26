
package eu.dnetlib.pace.tree.support;

import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.wcohen.ss.AbstractStringDistance;

public abstract class AbstractSortedComparator extends AbstractListComparator {

	/**
	 * Instantiates a new sorted second string compare algo.
	 *
	 * @param weight
	 *            the weight
	 * @param ssalgo
	 *            the ssalgo
	 */
	protected AbstractSortedComparator(final double weight, final AbstractStringDistance ssalgo) {
		super(weight, ssalgo);
	}

	protected AbstractSortedComparator(final Map<String, String> params, final AbstractStringDistance ssalgo) {
		super(Double.parseDouble(params.get("weight")), ssalgo);
	}

	@Override
	protected List<String> toList(final Object object) {
		if (object instanceof List) {
			List<String> fl = (List<String>) object;
			List<String> values = Lists.newArrayList(fl);
			Collections.sort(values);
			return values;
		}

		return Lists.newArrayList(object.toString());
	}
}
