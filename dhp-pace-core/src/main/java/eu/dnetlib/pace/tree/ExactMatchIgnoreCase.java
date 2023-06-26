
package eu.dnetlib.pace.tree;

import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;

import eu.dnetlib.pace.config.Config;
import eu.dnetlib.pace.tree.support.AbstractStringComparator;
import eu.dnetlib.pace.tree.support.ComparatorClass;

@ComparatorClass("exactMatchIgnoreCase")
public class ExactMatchIgnoreCase extends AbstractStringComparator {

	public ExactMatchIgnoreCase(Map<String, String> params) {
		super(params);
	}

	@Override
	public double compare(String a, String b, final Config conf) {

		if (a.isEmpty() || b.isEmpty())
			return -1;

		return a.equalsIgnoreCase(b) ? 1 : 0;
	}

	protected String toString(final Object object) {
		return toFirstString(object);
	}
}
