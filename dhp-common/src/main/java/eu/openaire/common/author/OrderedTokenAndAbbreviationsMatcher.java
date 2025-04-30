
package eu.openaire.common.author;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

/**
 * Utility class for comparing author names using token-based matching and abbreviation handling.
 *
 * <p>This class provides methods to tokenize author names and compare them based on
 * abbreviation recognition. It is useful for identifying name variations where part
 * of the full name might be reordered or abbreviated.</p>
 */
public class OrderedTokenAndAbbreviationsMatcher {
	/**
	 * Regular expression pattern used to split names into tokens.
	 *
	 * <p>The pattern matches spaces, punctuation symbols, and dashes, ensuring
	 * that names are split into meaningful components.</p>
	 */
	static public final Pattern SPLIT_REGEX = Pattern.compile("[\\s\\p{Punct}\\p{Pd}]+");

	/**
	 * Maximum allowed difference in the number of tokens between two names for them to be comparable.
	 */
	static public int NUM_TOKEN_MAX_DIFF = 2;

	/*
	 * Tokenizes a given name by splitting it using {@link #SPLIT_REGEX}, cleaning it, and sorting the tokens. <p>The
	 * cleaning ensures consistent representation of characters, and sorting helps in comparing tokens efficiently.</p>
	 * @param s The input string (author name).
	 * @return A sorted list of lowercase tokens derived from the input string.
	 */
	static private List<String> tokenize(String s) {
		return Stream
			.of(SPLIT_REGEX.split(StringUtils.stripAccents(s).toLowerCase(Locale.ROOT)))
			.filter(x -> !x.isEmpty())
			.sorted()
			.collect(Collectors.toList());
	}

	/**
	 * Compares two author names using token-based matching and abbreviation handling.
	 *
	 * <p>The comparison follows these rules:</p>
	 * <ul>
	 *   <li>Both names must have at least two tokens to be comparable.</li>
	 *   <li>The number of tokens between the two names should not differ by more than {@link #NUM_TOKEN_MAX_DIFF}.</li>
	 *   <li>Matching considers both full-token matches and abbreviation-based matches.</li>
	 * </ul>
	 *
	 * <p>The method returns an {@link Optional} containing a confidence score if a match is found,
	 * or an empty {@code Optional} if no match is identified.</p>
	 *
	 * @param a1 The first author name.
	 * @param a2 The second author name.
	 * @return An {@code Optional<Double>} with a confidence score (1.0 if a match is found), or empty if no match.
	 */
	// TODO: cercare prima i fulltokens e poi gli altri
	public static Optional<Double> compare(String a1, String a2) {
		if (a1 == null || a2 == null) {
			return Optional.empty();
		}

		List<String> a1_tokens = tokenize(a1);
		List<String> a2_tokens = tokenize(a2);

		int a1_num_tokens = a1_tokens.size();
		int a2_num_tokens = a2_tokens.size();

		// both authors must be composed of at least 2 elements to be comparable with this method
		if (a1_num_tokens < 2 || a2_num_tokens < 2)
			return Optional.empty();

		// both authors must not differ too much in number of elements to be comparable with this method
		if (Math.abs(a1_num_tokens - a2_num_tokens) > NUM_TOKEN_MAX_DIFF)
			return Optional.empty(); // use alternative comparison algo

		int a1_tokens_idx = 0;
		int a2_tokens_idx = 0;

		int shortMatches = 0;
		int longMatches = 0;
		int crossMatches = 0;

		// full tokens
		while (a1_tokens_idx < a1_tokens.size() && a2_tokens_idx < a2_tokens.size()) {
			String a1_curr_token = a1_tokens.get(a1_tokens_idx);
			if (a1_curr_token.length() < 2) {
				a1_tokens_idx++;
				continue;
			}
			String a2_curr_token = a2_tokens.get(a2_tokens_idx);
			if (a2_curr_token.length() < 2) {
				a2_tokens_idx++;
				continue;
			}

			int diff = a1_curr_token.compareTo(a2_curr_token);
			if (diff > 0) {
				a2_tokens_idx++;
			} else if (diff < 0) {
				a1_tokens_idx++;
			} else {
				longMatches++;
				a1_tokens.remove(a1_tokens_idx);
				a2_tokens.remove(a2_tokens_idx);
			}
		}

		a1_tokens_idx = 0;
		a2_tokens_idx = 0;

		while (a1_tokens_idx < a1_tokens.size() && a2_tokens_idx < a2_tokens.size()) {
			String a1_curr_token = a1_tokens.get(a1_tokens_idx);
			char a1_curr_token_initial = a1_curr_token.charAt(0);

			String a2_curr_token = a2_tokens.get(a2_tokens_idx);
			char a2_curr_token_initial = a2_curr_token.charAt(0);

			if (a1_curr_token_initial < a2_curr_token_initial) {
				// move ahead on a1 tokens
				a1_tokens_idx += 1;
			} else if (a1_curr_token_initial > a2_curr_token_initial) {
				// move ahead on a2 tokens
				a2_tokens_idx += 1;
			} else if (a1_curr_token.equals(a2_curr_token)) {
				if (a1_curr_token.length() > 1) {
					longMatches++;
				} else {
					shortMatches++;
				}
				a1_tokens_idx++;
				a2_tokens_idx++;
			} else if (a1_curr_token.length() == 1 || a2_curr_token.length() == 1) {
				// If one token is an initial, count it as a cross match
				crossMatches++;
				a1_tokens_idx++;
				a2_tokens_idx++;
			} else {
				// Move forward based on lexicographic order
				if (a1_curr_token.compareTo(a2_curr_token) < 0) {
					a1_tokens_idx++;
				} else {
					a2_tokens_idx++;
				}
			}
		}

		if (longMatches > 0
			&& (shortMatches + longMatches + crossMatches) == Math.min(a1_num_tokens, a2_num_tokens)) {
			double matchScore = (longMatches * 1.0 + shortMatches * 0.75 + crossMatches * 0.5)
				/ Math.max(a1_num_tokens, a2_num_tokens);
			return Optional.of(matchScore * 0.95);
		}

		return Optional.empty();
	}
}
