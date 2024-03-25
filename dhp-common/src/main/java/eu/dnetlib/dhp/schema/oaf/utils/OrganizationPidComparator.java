
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.Comparator;

import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class OrganizationPidComparator implements Comparator<StructuredProperty> {

	@Override
	public int compare(StructuredProperty left, StructuredProperty right) {

		PidType lClass = PidType.tryValueOf(left.getQualifier().getClassid());
		PidType rClass = PidType.tryValueOf(right.getQualifier().getClassid());

		if (lClass.equals(PidType.openorgs))
			return -1;
		if (rClass.equals(PidType.openorgs))
			return 1;

		if (lClass.equals(PidType.GRID))
			return -1;
		if (rClass.equals(PidType.GRID))
			return 1;

		if (lClass.equals(PidType.mag_id))
			return -1;
		if (rClass.equals(PidType.mag_id))
			return 1;

		if (lClass.equals(PidType.urn))
			return -1;
		if (rClass.equals(PidType.urn))
			return 1;

		return 0;
	}
}
