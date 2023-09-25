
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// https://ror.readme.io/docs/ror-identifier-pattern
public class RorCleaningRule {

	public static final String ROR_PREFIX = "https://ror.org/";

	private static final Pattern PATTERN = Pattern.compile("(?<ror>0[a-hj-km-np-tv-z|0-9]{6}[0-9]{2})");

	public static String clean(String ror) {
		String s = ror
			.replaceAll("\\s", "")
			.toLowerCase();

		Matcher m = PATTERN.matcher(s);

		if (m.find()) {
			return ROR_PREFIX + m.group("ror");
		}
		return "";
	}

}
