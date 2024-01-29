
package eu.dnetlib.dhp.schema.oaf.utils;

public class DoiCleaningRule {

	public static String clean(final String doi) {
		return doi
			.toLowerCase()
			.replaceAll("\\s", "")
			.replaceAll("^doi:", "")
			.replaceFirst(CleaningFunctions.DOI_PREFIX_REGEX, CleaningFunctions.DOI_PREFIX);
	}

}
