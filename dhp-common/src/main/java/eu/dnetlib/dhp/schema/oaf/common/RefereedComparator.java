
package eu.dnetlib.dhp.schema.oaf.common;

import java.util.Comparator;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Qualifier;

public class RefereedComparator implements Comparator<Qualifier> {

	@Override
	public int compare(Qualifier left, Qualifier right) {

		if (left == null && right == null)
			return 0;
		if (left == null)
			return 1;
		if (right == null)
			return -1;

		String lClass = left.getClassid();
		String rClass = right.getClassid();

		if (lClass.equals(rClass))
			return 0;

		if (lClass.equals(ModelConstants.PEER_REVIEWED_CLASSID))
			return -1;
		if (rClass.equals(ModelConstants.PEER_REVIEWED_CLASSID))
			return 1;

		if (lClass.equals(ModelConstants.NON_PEER_REVIEWED_CLASSID))
			return -1;
		if (rClass.equals(ModelConstants.NON_PEER_REVIEWED_CLASSID))
			return 1;

		if (lClass.equals(ModelConstants.UNKNOWN))
			return -1;
		if (rClass.equals(ModelConstants.UNKNOWN))
			return 1;

		// Else (but unlikely), lexicographical ordering will do.
		return lClass.compareTo(rClass);
	}
}
