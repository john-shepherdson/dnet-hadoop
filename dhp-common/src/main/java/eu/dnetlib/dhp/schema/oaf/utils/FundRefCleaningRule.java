
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FundRefCleaningRule {

	public static String clean(final String fundrefId) {

		String s = fundrefId
			.toLowerCase()
			.replaceAll("\\s", "");

		Matcher m = Pattern.compile("\\d+").matcher(s);
		if (m.matches()) {
			return m.group();
		} else {
			return "";
		}
	}

}
