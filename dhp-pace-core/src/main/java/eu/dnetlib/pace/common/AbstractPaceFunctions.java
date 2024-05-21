
package eu.dnetlib.pace.common;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.ibm.icu.text.Transliterator;

/**
 * Set of common functions for the framework
 *
 * @author claudio
 */
public class AbstractPaceFunctions extends PaceCommonUtils {

	// city map to be used when translating the city names into codes
	private static Map<String, String> cityMap = AbstractPaceFunctions
		.loadMapFromClasspath("/eu/dnetlib/pace/config/city_map.csv");

	// list of stopwords in different languages
	protected static Set<String> stopwords_gr = loadFromClasspath("/eu/dnetlib/pace/config/stopwords_gr.txt");
	protected static Set<String> stopwords_en = loadFromClasspath("/eu/dnetlib/pace/config/stopwords_en.txt");
	protected static Set<String> stopwords_de = loadFromClasspath("/eu/dnetlib/pace/config/stopwords_de.txt");
	protected static Set<String> stopwords_es = loadFromClasspath("/eu/dnetlib/pace/config/stopwords_es.txt");
	protected static Set<String> stopwords_fr = loadFromClasspath("/eu/dnetlib/pace/config/stopwords_fr.txt");
	protected static Set<String> stopwords_it = loadFromClasspath("/eu/dnetlib/pace/config/stopwords_it.txt");
	protected static Set<String> stopwords_pt = loadFromClasspath("/eu/dnetlib/pace/config/stopwords_pt.txt");

	// blacklist of ngrams: to avoid generic keys
	protected static Set<String> ngramBlacklist = loadFromClasspath("/eu/dnetlib/pace/config/ngram_blacklist.txt");

	// html regex for normalization
	public static final Pattern HTML_REGEX = Pattern.compile("<[^>]*>");

	private static final String alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";

	// doi prefix for normalization
	public static final Pattern DOI_PREFIX = Pattern.compile("(https?:\\/\\/dx\\.doi\\.org\\/)|(doi:)");

	private static Pattern numberPattern = Pattern.compile("-?\\d+(\\.\\d+)?");

	private static Pattern hexUnicodePattern = Pattern.compile("\\\\u(\\p{XDigit}{4})");

	private static Pattern romanNumberPattern = Pattern
		.compile("^M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$");

	protected static String concat(final List<String> l) {
		return Joiner.on(" ").skipNulls().join(l);
	}

	public static String cleanup(final String s) {
		final String s1 = HTML_REGEX.matcher(s).replaceAll("");
		final String s2 = unicodeNormalization(s1.toLowerCase());
		final String s3 = nfd(s2);
		final String s4 = fixXML(s3);
		final String s5 = s4.replaceAll("([0-9]+)", " $1 ");
		final String s6 = transliterate(s5);
		final String s7 = fixAliases(s6);
		final String s8 = s7.replaceAll("[^\\p{ASCII}]", "");
		final String s9 = s8.replaceAll("[\\p{Punct}]", " ");
		final String s10 = s9.replaceAll("\\n", " ");
		final String s11 = s10.replaceAll("(?m)\\s+", " ");
		final String s12 = s11.trim();
		return s12;
	}

	protected static String fixXML(final String a) {

		return a
			.replaceAll("&ndash;", " ")
			.replaceAll("&amp;", " ")
			.replaceAll("&quot;", " ")
			.replaceAll("&minus;", " ");
	}

	protected static boolean checkNumbers(final String a, final String b) {
		final String numbersA = getNumbers(a);
		final String numbersB = getNumbers(b);
		final String romansA = getRomans(a);
		final String romansB = getRomans(b);
		return !numbersA.equals(numbersB) || !romansA.equals(romansB);
	}

	protected static String getRomans(final String s) {
		final StringBuilder sb = new StringBuilder();
		for (final String t : s.split(" ")) {
			sb.append(isRoman(t) ? t : "");
		}
		return sb.toString();
	}

	protected static boolean isRoman(final String s) {
		Matcher m = romanNumberPattern.matcher(s);
		return m.matches() && m.hitEnd();
	}

	protected static String getNumbers(final String s) {
		final StringBuilder sb = new StringBuilder();
		for (final String t : s.split(" ")) {
			sb.append(isNumber(t) ? t : "");
		}
		return sb.toString();
	}

	public static boolean isNumber(String strNum) {
		if (strNum == null) {
			return false;
		}
		return numberPattern.matcher(strNum).matches();
	}

	protected static String removeSymbols(final String s) {
		final StringBuilder sb = new StringBuilder();

		s.chars().forEach(ch -> {
			sb.append(StringUtils.contains(alpha, ch) ? (char) ch : ' ');
		});

		return sb.toString().replaceAll("\\s+", " ");
	}

	protected static boolean notNull(final String s) {
		return s != null;
	}

	public static String utf8(final String s) {
		byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
		return new String(bytes, StandardCharsets.UTF_8);
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

	protected static String filterStopWords(final String s, final Set<String> stopwords) {
		final StringTokenizer st = new StringTokenizer(s);
		final StringBuilder sb = new StringBuilder();
		while (st.hasMoreTokens()) {
			final String token = st.nextToken();
			if (!stopwords.contains(token)) {
				sb.append(token);
				sb.append(" ");
			}
		}
		return sb.toString().trim();
	}

	public static String filterAllStopWords(String s) {

		s = filterStopWords(s, stopwords_en);
		s = filterStopWords(s, stopwords_de);
		s = filterStopWords(s, stopwords_it);
		s = filterStopWords(s, stopwords_fr);
		s = filterStopWords(s, stopwords_pt);
		s = filterStopWords(s, stopwords_es);
		s = filterStopWords(s, stopwords_gr);

		return s;
	}

	protected static Collection<String> filterBlacklisted(final Collection<String> set,
		final Set<String> ngramBlacklist) {
		final Set<String> newset = Sets.newLinkedHashSet();
		for (final String s : set) {
			if (!ngramBlacklist.contains(s)) {
				newset.add(s);
			}
		}
		return newset;
	}

	public static Map<String, String> loadMapFromClasspath(final String classpath) {

		Transliterator transliterator = Transliterator.getInstance("Any-Eng");

		final Map<String, String> m = new HashMap<>();
		try {
			for (final String s : IOUtils
				.readLines(AbstractPaceFunctions.class.getResourceAsStream(classpath), StandardCharsets.UTF_8)) {
				// string is like this: code;word1;word2;word3
				String[] line = s.split(";");
				String value = line[0];
				for (int i = 1; i < line.length; i++) {
					m.put(fixAliases(transliterator.transliterate(line[i].toLowerCase())), value);
				}
			}
		} catch (final Throwable e) {
			return new HashMap<>();
		}
		return m;
	}

	public static String removeKeywords(String s, Set<String> keywords) {

		s = " " + s + " ";
		for (String k : keywords) {
			s = s.replaceAll(k.toLowerCase(), "");
		}

		return s.trim();
	}

	public static double commonElementsPercentage(Set<String> s1, Set<String> s2) {

		double longer = Math.max(s1.size(), s2.size());
		return (double) s1.stream().filter(s2::contains).count() / longer;
	}

	// convert the set of keywords to codes
	public static Set<String> toCodes(Set<String> keywords, Map<String, String> translationMap) {
		return keywords.stream().map(s -> translationMap.get(s)).collect(Collectors.toSet());
	}

	public static Set<String> keywordsToCodes(Set<String> keywords, Map<String, String> translationMap) {
		return toCodes(keywords, translationMap);
	}

	public static Set<String> citiesToCodes(Set<String> keywords) {
		return toCodes(keywords, cityMap);
	}

	protected static String firstLC(final String s) {
		return StringUtils.substring(s, 0, 1).toLowerCase();
	}

	public static String normalizePid(String pid) {
		return DOI_PREFIX.matcher(pid.toLowerCase()).replaceAll("");
	}

	// get the list of keywords into the input string
	public static Set<String> getKeywords(String s1, Map<String, String> translationMap, int windowSize) {

		String s = s1;

		List<String> tokens = Arrays.asList(s.toLowerCase().split(" "));

		Set<String> codes = new HashSet<>();

		if (tokens.size() < windowSize)
			windowSize = tokens.size();

		int length = windowSize;

		while (length != 0) {

			for (int i = 0; i <= tokens.size() - length; i++) {
				String candidate = concat(tokens.subList(i, i + length));
				if (translationMap.containsKey(candidate)) {
					codes.add(candidate);
					s = s.replace(candidate, "").trim();
				}
			}

			tokens = Arrays.asList(s.split(" "));
			length -= 1;
		}

		return codes;
	}

	public static Set<String> getCities(String s1, int windowSize) {
		return getKeywords(s1, cityMap, windowSize);
	}

	public static <T> String readFromClasspath(final String filename, final Class<T> clazz) {
		final StringWriter sw = new StringWriter();
		try {
			IOUtils.copy(clazz.getResourceAsStream(filename), sw, StandardCharsets.UTF_8);
			return sw.toString();
		} catch (final IOException e) {
			throw new RuntimeException("cannot load resource from classpath: " + filename);
		}
	}

}
