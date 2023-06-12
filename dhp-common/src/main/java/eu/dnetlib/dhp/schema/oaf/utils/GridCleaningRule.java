
package eu.dnetlib.dhp.schema.oaf.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GridCleaningRule {

	public static String clean(String grid) {
		String s = grid
			.replaceAll("\\s", "")
			.toLowerCase();

		Matcher m = Pattern.compile("\\d{4,6}\\.[0-9a-z]{1,2}").matcher(s);
		return m.matches() ? "grid." + m.group() : "";
	}

}
