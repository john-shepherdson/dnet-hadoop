
package eu.dnetlib.pace.tree;

import java.util.Map;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractStringComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("maxLengthMatch")
public class MaxLengthMatch extends AbstractStringComparator {

	private final int limit;

	public MaxLengthMatch(Map<String, String> params) {
		super(params);

		limit = Integer.parseInt(params.getOrDefault("limit", "200"));
	}

	@Override
	public double compare(String a, String b, final Config conf) {
		return a.length() < limit && b.length() < limit ? 1.0 : -1.0;
	}

	protected String toString(final Object object) {
		return toFirstString(object);
	}
}
