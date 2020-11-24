
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.Comparator;

import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class ResultPidComparator implements Comparator<StructuredProperty> {

	@Override
	public int compare(StructuredProperty left, StructuredProperty right) {

		PidType lClass = PidType.valueOf(left.getQualifier().getClassid());
		PidType rClass = PidType.valueOf(right.getQualifier().getClassid());

		if (lClass.equals(PidType.doi))
			return -1;
		if (rClass.equals(PidType.doi))
			return 1;

		if (lClass.equals(PidType.pmid))
			return -1;
		if (rClass.equals(PidType.pmid))
			return 1;

		if (lClass.equals(PidType.pmc))
			return -1;
		if (rClass.equals(PidType.pmc))
			return 1;

		if (lClass.equals(PidType.handle))
			return -1;
		if (rClass.equals(PidType.handle))
			return 1;

		if (lClass.equals(PidType.arXiv))
			return -1;
		if (rClass.equals(PidType.arXiv))
			return 1;

		if (lClass.equals(PidType.NCID))
			return -1;
		if (rClass.equals(PidType.NCID))
			return 1;

		if (lClass.equals(PidType.GBIF))
			return -1;
		if (rClass.equals(PidType.GBIF))
			return 1;

		if (lClass.equals(PidType.nct))
			return -1;
		if (rClass.equals(PidType.nct))
			return 1;

		if (lClass.equals(PidType.urn))
			return -1;
		if (rClass.equals(PidType.urn))
			return 1;

		return 0;
	}
}
