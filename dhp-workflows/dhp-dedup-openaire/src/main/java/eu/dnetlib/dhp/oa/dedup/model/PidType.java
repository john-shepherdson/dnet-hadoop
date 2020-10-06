
package eu.dnetlib.dhp.oa.dedup.model;

public enum PidType {

	// from the less to the more important
	undefined, original, orcid, ror, grid, pdb, arXiv, pmid, pmc, doi;

	public static PidType classidValueOf(String s) {
		try {
			return PidType.valueOf(s);
		} catch (Exception e) {
			return PidType.undefined;
		}
	}

}
