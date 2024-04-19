
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.Comparator;

import eu.dnetlib.dhp.schema.oaf.Qualifier;

/**
 * Comparator for sorting the values from the dnet:review_levels vocabulary, implements the following ordering
 *
 * peerReviewed (0001) > nonPeerReviewed (0002) > UNKNOWN (0000)
 */
public class RefereedComparator implements Comparator<Qualifier> {

	@Override
	public int compare(Qualifier left, Qualifier right) {
		if (left == null || left.getClassid() == null) {
			return (right == null || right.getClassid() == null) ? 0 : -1;
		} else if (right == null || right.getClassid() == null) {
			return 1;
		}

		String lClass = left.getClassid();
		String rClass = right.getClassid();

		if (lClass.equals(rClass))
			return 0;

		if ("0001".equals(lClass))
			return -1;
		if ("0001".equals(rClass))
			return 1;

		if ("0002".equals(lClass))
			return -1;
		if ("0002".equals(rClass))
			return 1;

		if ("0000".equals(lClass))
			return -1;
		if ("0000".equals(rClass))
			return 1;

		return 0;
	}
}
