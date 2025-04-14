
package eu.openaire.common.author;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Helper class providing methods to match authors by their names.
 */
public class AuthorMatchers {

	/**
	 * Checks whether two author names are equal, ignoring case differences.
	 *
	 * @param a1 The first author name.
	 * @param a2 The second author name.
	 * @return {@code true} if the names are equal (case-insensitive), otherwise {@code false}.
	 */
	static public boolean matchEqualsIgnoreCase(String a1, String a2) {
		if (a1 == null || a2 == null)
			return false;
		else
			return a1 == a2 || a1.toLowerCase(Locale.ROOT).equals(a2.toLowerCase(Locale.ROOT));
	}

	/**
	 * Checks whether two author names match based on ordered tokens and abbreviations.
	 *
	 * <p>This method leverages the {@code OrderedTokenAndAbbreviationsMatcher} to compare names
	 * by considering token order and abbreviation expansion.</p>
	 *
	 * @param a1 The first author name.
	 * @param a2 The second author name.
	 * @return An {@code Optional<Double>} with a confidence score (1.0 if a match is found), or empty if no match.
	 */
	static public Optional<Double> matchOrderedTokenAndAbbreviations(String a1, String a2) {
		return OrderedTokenAndAbbreviationsMatcher.compare(a1, a2);
	}

	/**
	 * Removes matching authors from two lists using a custom matching function.
	 *
	 * <p>This method iterates through the lists of authors and candidate authors, applying the
	 * given matching function to determine matches. If a match is found, both elements are removed
	 * from their respective lists.</p>
	 *
	 * @param authors           The list of unmatched authors.
	 * @param candidate_authors The list of candidate authors.
	 * @param matchingFunc      A function that determines whether two author names match.
	 * @return A list containing matched author-candidate pairs.
	 */
	static public List<String> removeMatches(
			List<String> authors,
			List<String> candidate_authors,
			BiFunction<String, String, Boolean> matchingFunc) {
		List<String> matched = new ArrayList<>();

		if (authors != null && !authors.isEmpty()) {
			Iterator<String> ait = authors.iterator();

			while (ait.hasNext()) {
				String author = ait.next();
				Iterator<String> oit = candidate_authors.iterator();

				while (oit.hasNext()) {
					String candidate = oit.next();

					if (matchingFunc.apply(author, candidate)) {
						ait.remove();
						oit.remove();

						matched.add(author);
						matched.add(candidate);

						break;
					}
				}

			}
		}

		return matched;
	}

	/**
	 * Finds matches between a list of unmatched authors and a list of candidate authors using a sequence of matching steps.
	 *
	 * <p>Each step in the list of {@link AuthorMatcherStep} objects applies a specific matching strategy. The result
	 * is a list of successful matches.</p>
	 *
	 * @param authors           The list of unmatched authors.
	 * @param candidate_authors The list of candidate authors.
	 * @param steps             The list of matching steps to apply.
	 * @param <UA>              The type representing the unmatched author.
	 * @param <CA>              The type representing the candidate author.
	 * @return A list of {@link AuthorMatch} objects representing the matches found.
	 */
	static public <UA, CA> List<AuthorMatch<UA, CA>> findMatches(
			List<UA> authors,
			List<CA> candidate_authors,
			List<AuthorMatcherStep<UA, CA>> steps) {
		List<AuthorMatch<UA, CA>> result = new ArrayList<>();
		List<UA> unmatched_authors = new ArrayList<>(authors);
		List<CA> unmatched_candidates = new ArrayList<>(candidate_authors);

		for (AuthorMatcherStep<UA, CA> s : steps) {
			result
					.addAll(
							evaluateStep(
									unmatched_authors,
									unmatched_candidates,
									s));
		}
		return result;
	}

	static private <UA, CA> List<AuthorMatch<UA, CA>> evaluateStep(
			List<UA> unmatched_authors,
			List<CA> candidate_authors,
			AuthorMatcherStep<UA, CA> step) {
		List<AuthorMatch<UA, CA>> result = new ArrayList<>();
		if (unmatched_authors.isEmpty()) {
			return result;
		}

		Iterator<CA> oit = candidate_authors.iterator();
		while (oit.hasNext()) {
			CA candidate = oit.next();

			List<AuthorMatch<UA, CA>> potential_matches = unmatched_authors
					.stream()
					.map(x -> step.getMatchingFunc().apply(x, candidate))
					.filter(Optional::isPresent)
					.map(Optional::get)
					.sorted(new Comparator<AuthorMatch<UA, CA>>() {
						@Override
						public int compare(AuthorMatch<UA, CA> o1, AuthorMatch<UA, CA> o2) {
							return Double.compare(o1.getConfidence(), o2.getConfidence());
						}
					})
					.collect(Collectors.toList());

			if (potential_matches.size() == 1 ||
					(potential_matches.size() > 1
							&& (step.getExclusionPredicate() == null
							|| !step.getExclusionPredicate().test(potential_matches)))) {
				AuthorMatch<UA, CA> m = potential_matches.get(0);
				Optional<AuthorMatch<UA, CA>> existing = result.stream().filter(
						x -> x.getMatchedAuthor().equals(x)).findFirst();
				if (existing.isPresent()) {
					if (existing.get().getConfidence() < m.getConfidence()) {
						result.remove(existing.get());
						result.add(m);
					}
				} else {
					result.add(m);
				}

				oit.remove();
			}

		}

		unmatched_authors.removeAll(result.stream().map(AuthorMatch::getMatchedAuthor).collect(Collectors.toList()));

		return result;
	}
}
