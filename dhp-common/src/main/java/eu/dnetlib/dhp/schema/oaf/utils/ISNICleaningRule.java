
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// https://www.wikidata.org/wiki/Property:P213
public class ISNICleaningRule {

	public static String clean(final String isni) {

		Matcher m = Pattern.compile("([0]{4}) ?([0-9]{4}) ?([0-9]{4}) ?([0-9]{3}[0-9X])").matcher(isni);
		if (m.matches()) {
			return String.join("", m.group(1), m.group(2), m.group(3), m.group(4));
		} else {
			return "";
		}
	}
}
