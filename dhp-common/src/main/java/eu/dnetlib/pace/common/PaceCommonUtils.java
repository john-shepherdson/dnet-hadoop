
package eu.dnetlib.pace.common;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.ibm.icu.text.Transliterator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Set of common functions for the framework
 *
 * @author claudio
 */
public class PaceCommonUtils {

	// transliterator
	protected static Transliterator transliterator = Transliterator.getInstance("Any-Eng");

	protected static final String aliases_from = "⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻⁼⁽⁾ⁿ₀₁₂₃₄₅₆₇₈₉₊₋₌₍₎àáâäæãåāèéêëēėęəîïíīįìôöòóœøōõûüùúūßśšłžźżçćčñń";
	protected static final String aliases_to = "0123456789+-=()n0123456789+-=()aaaaaaaaeeeeeeeeiiiiiioooooooouuuuussslzzzcccnn";

	protected static Pattern hexUnicodePattern = Pattern.compile("\\\\u(\\p{XDigit}{4})");

	protected static String fixAliases(final String s) {
		final StringBuilder sb = new StringBuilder();

		s.chars().forEach(ch -> {
			final int i = StringUtils.indexOf(aliases_from, ch);
			sb.append(i >= 0 ? aliases_to.charAt(i) : (char) ch);
		});

		return sb.toString();
	}

	protected static String transliterate(final String s) {
		try {
			return transliterator.transliterate(s);
		} catch (Exception e) {
			return s;
		}
	}

	public static String normalize(final String s) {
		return fixAliases(transliterate(nfd(unicodeNormalization(s))))
			.toLowerCase()
			// do not compact the regexes in a single expression, would cause StackOverflowError in case of large input
			// strings
			.replaceAll("[^ \\w]+", "")
			.replaceAll("(\\p{InCombiningDiacriticalMarks})+", "")
			.replaceAll("(\\p{Punct})+", " ")
			.replaceAll("(\\d)+", " ")
			.replaceAll("(\\n)+", " ")
			.trim();
	}

	public static String nfd(final String s) {
		return Normalizer.normalize(s, Normalizer.Form.NFD);
	}

	public static String unicodeNormalization(final String s) {

		Matcher m = hexUnicodePattern.matcher(s);
		StringBuffer buf = new StringBuffer(s.length());
		while (m.find()) {
			String ch = String.valueOf((char) Integer.parseInt(m.group(1), 16));
			m.appendReplacement(buf, Matcher.quoteReplacement(ch));
		}
		m.appendTail(buf);
		return buf.toString();
	}

	public static Set<String> loadFromClasspath(final String classpath) {

		Transliterator transliterator = Transliterator.getInstance("Any-Eng");

		final Set<String> h = Sets.newHashSet();
		try {
			for (final String s : IOUtils
				.readLines(PaceCommonUtils.class.getResourceAsStream(classpath), StandardCharsets.UTF_8)) {
				h.add(fixAliases(transliterator.transliterate(s))); // transliteration of the stopwords
			}
		} catch (final Throwable e) {
			return Sets.newHashSet();
		}
		return h;
	}

	protected static Iterable<String> tokens(final String s, final int maxTokens) {
		return Iterables.limit(Splitter.on(" ").omitEmptyStrings().trimResults().split(s), maxTokens);
	}

}
