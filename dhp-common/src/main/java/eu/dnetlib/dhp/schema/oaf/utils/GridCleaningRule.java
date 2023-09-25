
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GridCleaningRule {

	public static final Pattern PATTERN = Pattern.compile("(?<grid>\\d{4,6}\\.[0-9a-z]{1,2})");

	public static String clean(String grid) {
		String s = grid
			.replaceAll("\\s", "")
			.toLowerCase();

		Matcher m = PATTERN.matcher(s);
		if (m.find()) {
			return "grid." + m.group("grid");
		}

		return "";
	}

}
