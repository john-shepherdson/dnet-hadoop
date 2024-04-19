
package eu.dnetlib.dhp.schema.oaf.utils;

import org.apache.commons.lang3.StringUtils;

public class DoiCleaningRule {

	public static String clean(final String doi) {
		return doi
			.toLowerCase()
			.replaceAll("\\s", "")
			.replaceAll("^doi:", "")
			.replaceFirst(CleaningFunctions.DOI_PREFIX_REGEX, CleaningFunctions.DOI_PREFIX);
	}

	public static String normalizeDoi(final String input) {
		if (input == null)
			return null;
		final String replaced = input
			.replaceAll("\\n|\\r|\\t|\\s", "")
			.toLowerCase()
			.replaceFirst(CleaningFunctions.DOI_PREFIX_REGEX, CleaningFunctions.DOI_PREFIX);
		if (StringUtils.isEmpty(replaced))
			return null;

		if (!replaced.contains("10."))
			return null;

		final String ret = replaced.substring(replaced.indexOf("10."));

		if (!ret.startsWith(CleaningFunctions.DOI_PREFIX))
			return null;

		return ret;

	}

}
