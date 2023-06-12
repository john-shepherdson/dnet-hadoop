
package eu.dnetlib.dhp.schema.oaf.utils;

// https://researchguides.stevens.edu/c.php?g=442331&p=6577176
public class PmidCleaningRule {

	public static String clean(String pmid) {
		String s = pmid
			.toLowerCase()
			.replaceAll("\\s", "")
			.trim()
			.replaceAll("^0+", "");
		return s.matches("^\\d{1,8}$") ? s : "";
	}

}
