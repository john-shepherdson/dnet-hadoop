
package eu.dnetlib.pace.tree;

import java.util.Map;

import eu.dnetlib.pace.tree.support.ComparatorClass;

/**
 * The Class ExactMatch.
 *
 * @author claudio
 */
@ComparatorClass("doiExactMatch")
public class DoiExactMatch extends ExactMatchIgnoreCase {

	public final String PREFIX = "(http:\\/\\/dx\\.doi\\.org\\/)|(doi:)";

	public DoiExactMatch(final Map<String, String> params) {
		super(params);
	}

	@Override
	protected String toString(final Object f) {
		return super.toString(f).replaceAll(PREFIX, "");
	}

}
