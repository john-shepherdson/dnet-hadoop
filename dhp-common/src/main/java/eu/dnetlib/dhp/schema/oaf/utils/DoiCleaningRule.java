
package eu.dnetlib.dhp.schema.oaf.utils;

import org.apache.commons.lang3.StringUtils;

public class DoiCleaningRule {

	public static String clean(final String doi) {
		if (doi == null)
			return null;
		final String replaced = doi
			.replaceAll("\\n|\\r|\\t|\\s", "")
			.replaceAll("^doi:", "")
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
