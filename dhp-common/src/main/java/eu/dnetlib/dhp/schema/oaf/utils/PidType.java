
package eu.dnetlib.dhp.schema.oaf.utils;

import org.apache.commons.lang3.EnumUtils;

public enum PidType {

	// Result
	doi, pmid, pmc, handle, arXiv, NCID, GBIF, nct, pdb,

	// Organization
	GRID, mag_id, urn;

	public static boolean isValid(String type) {
		return EnumUtils.isValidEnum(PidType.class, type);
	}

}
