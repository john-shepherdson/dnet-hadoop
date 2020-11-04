
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.Comparator;

public class ResultPidComparator implements Comparator<PidType> {

	@Override
	public int compare(PidType pLeft, PidType pRight) {
		if (pLeft.equals(PidType.doi))
			return -1;
		if (pRight.equals(PidType.doi))
			return 1;

		if (pLeft.equals(PidType.pmid))
			return -1;
		if (pRight.equals(PidType.pmid))
			return 1;

		if (pLeft.equals(PidType.pmc))
			return -1;
		if (pRight.equals(PidType.pmc))
			return 1;

		if (pLeft.equals(PidType.handle))
			return -1;
		if (pRight.equals(PidType.handle))
			return 1;

		if (pLeft.equals(PidType.arXiv))
			return -1;
		if (pRight.equals(PidType.arXiv))
			return 1;

		if (pLeft.equals(PidType.NCID))
			return -1;
		if (pRight.equals(PidType.NCID))
			return 1;

		if (pLeft.equals(PidType.GBIF))
			return -1;
		if (pRight.equals(PidType.GBIF))
			return 1;

		if (pLeft.equals(PidType.nct))
			return -1;
		if (pRight.equals(PidType.nct))
			return 1;

		if (pLeft.equals(PidType.urn))
			return -1;
		if (pRight.equals(PidType.urn))
			return 1;

		return 0;
	}
}
