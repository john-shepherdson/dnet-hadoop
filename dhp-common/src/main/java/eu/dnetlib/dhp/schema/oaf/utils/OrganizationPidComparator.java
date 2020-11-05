
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.Comparator;

public class OrganizationPidComparator implements Comparator<PidType> {

	@Override
	public int compare(PidType pLeft, PidType pRight) {
		if (pLeft.equals(PidType.GRID))
			return -1;
		if (pRight.equals(PidType.GRID))
			return 1;

		if (pLeft.equals(PidType.mag_id))
			return -1;
		if (pRight.equals(PidType.mag_id))
			return 1;

		if (pLeft.equals(PidType.urn))
			return -1;
		if (pRight.equals(PidType.urn))
			return 1;

		return 0;
	}
}
