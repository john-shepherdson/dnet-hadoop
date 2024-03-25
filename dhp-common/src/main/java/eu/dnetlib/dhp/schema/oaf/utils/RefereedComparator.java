
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.Comparator;

import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

/**
 * Comparator for sorting the values from the dnet:review_levels vocabulary, implements the following ordering
 *
 * peerReviewed (0001) > nonPeerReviewed (0002) > UNKNOWN (0000)
 */
public class RefereedComparator implements Comparator<Qualifier> {

	@Override
	public int compare(Qualifier left, Qualifier right) {

		String lClass = left.getClassid();
		String rClass = right.getClassid();

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
