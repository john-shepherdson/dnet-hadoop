
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// https://researchguides.stevens.edu/c.php?g=442331&p=6577176
public class PmidCleaningRule {

	public static final Pattern PATTERN = Pattern.compile("[1-9]{1,8}");

	public static String clean(String pmid) {
		String s = pmid
			.toLowerCase()
			.replaceAll("\\s", "");

		final Matcher m = PATTERN.matcher(s);

		if (m.find()) {
			return m.group();
		}
		return "";
	}

}
