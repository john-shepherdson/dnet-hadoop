
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PmcCleaningRule {

	public static final Pattern PATTERN = Pattern.compile("PMC\\d{1,8}");

	public static String clean(String pmc) {
		String s = pmc
			.replaceAll("\\s", "")
			.toUpperCase();

		final Matcher m = PATTERN.matcher(s);

		if (m.find()) {
			return m.group();
		}
		return "";
	}

}
