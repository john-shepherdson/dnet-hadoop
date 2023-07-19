
package eu.dnetlib.dhp.schema.oaf.utils;

public class PmcCleaningRule {

	public static String clean(String pmc) {
		String s = pmc
			.replaceAll("\\s", "")
			.toUpperCase();
		return s.matches("^PMC\\d{1,8}$") ? s : "";
	}

}
