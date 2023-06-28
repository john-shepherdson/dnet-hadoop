
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PICCleaningRule {

	public static String clean(final String pic) {

		Matcher m = Pattern.compile("\\d{9}").matcher(pic);
		if (m.matches()) {
			return m.group();
		} else {
			return "";
		}
	}

}
