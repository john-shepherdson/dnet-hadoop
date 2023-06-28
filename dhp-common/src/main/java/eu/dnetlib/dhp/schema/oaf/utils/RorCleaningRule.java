
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// https://ror.readme.io/docs/ror-identifier-pattern
public class RorCleaningRule {

	public static String clean(String ror) {
		String s = ror
			.replaceAll("\\s", "")
			.toLowerCase();
		Matcher m = Pattern.compile("0[a-hj-km-np-tv-z|0-9]{6}[0-9]{2}").matcher(s);
		return m.matches() ? "https://ror.org/" + m.group() : "";
	}

}
