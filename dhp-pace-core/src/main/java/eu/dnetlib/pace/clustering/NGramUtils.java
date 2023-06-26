
package eu.dnetlib.pace.clustering;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.pace.common.AbstractPaceFunctions;

public class NGramUtils extends AbstractPaceFunctions {
	static private final NGramUtils NGRAMUTILS = new NGramUtils();

	private static final int SIZE = 100;

	private static final Set<String> stopwords = AbstractPaceFunctions
		.loadFromClasspath("/eu/dnetlib/pace/config/stopwords_en.txt");

	public static String cleanupForOrdering(String s) {
		String result = NGRAMUTILS.filterStopWords(NGRAMUTILS.normalize(s), stopwords);
		return result.isEmpty() ? result : result.replace(" ", "");
	}

}
