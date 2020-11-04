
package eu.dnetlib.dhp.schema.oaf.utils;

import org.apache.commons.lang3.EnumUtils;

public enum PidType {

	// Result
	doi, pmid, pmc, handle, arXiv, NCID, GBIF, nct, pdb,

	// Organization
	GRID, mag_id, urn,

	// Used by dedup
	undefined, original;

	public static boolean isValid(String type) {
		return EnumUtils.isValidEnum(PidType.class, type);
	}

	public static PidType tryValueOf(String s) {
		try {
			return PidType.valueOf(s);
		} catch (Exception e) {
			return PidType.original;
		}
	}

}
