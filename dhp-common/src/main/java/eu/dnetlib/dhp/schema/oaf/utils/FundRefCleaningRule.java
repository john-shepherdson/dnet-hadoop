
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FundRefCleaningRule {

	public static final Pattern PATTERN = Pattern.compile("\\d+");

	public static String clean(final String fundRefId) {

		String s = fundRefId
			.toLowerCase()
			.replaceAll("\\s", "");

		Matcher m = PATTERN.matcher(s);
		if (m.find()) {
			return m.group();
		} else {
			return "";
		}
	}

}
