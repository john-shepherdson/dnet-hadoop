
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PICCleaningRule {

	public static final Pattern PATTERN = Pattern.compile("\\d{9}");

	public static String clean(final String pic) {

		Matcher m = PATTERN.matcher(pic);
		if (m.find()) {
			return m.group();
		} else {
			return "";
		}
	}

}
