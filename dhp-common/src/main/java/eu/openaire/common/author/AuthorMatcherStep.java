
package eu.openaire.common.author;

import static eu.openaire.common.author.AuthorMatchers.matchOrderedTokenAndAbbreviations;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Represents a step in the author matching process, which applies a specific matching logic
 * between an unmatched author (UA) and a candidate author (CA).
 *
 * <p>This class encapsulates a matching function, an exclusion predicate, and a name for the
 * matching step. It allows defining different strategies for comparing authors, such as full name
 * matching and abbreviation-based matching.</p>
 *
 * @param <UA> The type representing the unmatched author.
 * @param <CA> The type representing the candidate author.
 */
public class AuthorMatcherStep<UA, CA> {
	private final BiFunction<UA, CA, Optional<AuthorMatch<UA, CA>>> matchingFunc;
	private final Predicate<List<AuthorMatch<UA, CA>>> exclusionPredicate;
	private final String name;

	private AuthorMatcherStep(BiFunction<UA, CA, Optional<AuthorMatch<UA, CA>>> matchingFunc,
		Predicate<List<AuthorMatch<UA, CA>>> exclusionPredicate, String name) {
		this.matchingFunc = matchingFunc;
		this.exclusionPredicate = exclusionPredicate;
		this.name = name;
	}

	/**
	 * Creates a builder for a matching step that compares strings ignoring case.
	 *
	 * @param ex1  Function to extract the string from an unmatched author.
	 * @param ex2  Function to extract the string from a candidate author.
	 * @param <UA> The type of the unmatched author.
	 * @param <CA> The type of the candidate author.
	 * @return A builder to further configure the matching step.
	 */
	public static <UA, CA> Builder<UA, CA> stringIgnoreCaseMatcher(Function<UA, String> ex1, Function<CA, String> ex2) {
		return new Builder<UA, CA>()
			.matchingFunc((ua, ca) -> {
				String author = ex1.apply(ua);
				String candidate = ex2.apply(ca);

				if (author == null || candidate == null)
					return Optional.empty();
				else if (author.toLowerCase(Locale.ROOT).equals(candidate.toLowerCase(Locale.ROOT))) {
					return Optional.of(new AuthorMatch<>(ua, ca, "", 1));
				}
				return Optional.empty();
			});
	}

	/**
	 * Creates a builder for a matching step that compares names based on abbreviations.
	 *
	 * @param ex1  Function to extract the author name from an unmatched author.
	 * @param ex2  Function to extract the author name from a candidate author.
	 * @param <UA> The type of the unmatched author.
	 * @param <CA> The type of the candidate author.
	 * @return A builder to further configure the matching step.
	 */
	public static <UA, CA> Builder<UA, CA> abbreviationsMatcher(Function<UA, String> ex1, Function<CA, String> ex2) {
		return new Builder<UA, CA>()
			.name("abbreviations")
			.matchingFunc((ua, ca) -> {
				String author = ex1.apply(ua);
				String candidate = ex2.apply(ca);

				return matchOrderedTokenAndAbbreviations(author, candidate)
					.map(confidence -> new AuthorMatch<>(ua, ca, "", confidence));
			});
	}

	/**
	 * Gets the matching function used in this step.
	 *
	 * @return The matching function.
	 */
	public BiFunction<UA, CA, Optional<AuthorMatch<UA, CA>>> getMatchingFunc() {
		return matchingFunc;
	}

	/**
	 * Gets the exclusion predicate used in this step.
	 *
	 * @return The exclusion predicate.
	 */
	public Predicate<List<AuthorMatch<UA, CA>>> getExclusionPredicate() {
		return exclusionPredicate;
	}

	/**
	 * Gets the name of this matching step.
	 *
	 * @return The name of the matching step.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Builder class for constructing an {@link AuthorMatcherStep}.
	 *
	 * @param <UA> The type of the unmatched author.
	 * @param <CA> The type of the candidate author.
	 */
	public static class Builder<UA, CA> {
		private BiFunction<UA, CA, Optional<AuthorMatch<UA, CA>>> matchingFunc;
		private Predicate<List<AuthorMatch<UA, CA>>> exclusionPredicate;
		private String name;

		/**
		 * Sets the matching function for this builder.
		 *
		 * @param matchingFunc The matching function to use.
		 * @return This builder instance.
		 */
		public Builder<UA, CA> matchingFunc(BiFunction<UA, CA, Optional<AuthorMatch<UA, CA>>> matchingFunc) {
			this.matchingFunc = matchingFunc;
			return this;
		}

		/**
		 * Sets the exclusion predicate for this builder.
		 *
		 * @param exclusionPredicate The exclusion predicate to use.
		 * @return This builder instance.
		 */
		public Builder<UA, CA> exclusionPredicate(Predicate<List<AuthorMatch<UA, CA>>> exclusionPredicate) {
			this.exclusionPredicate = exclusionPredicate;
			return this;
		}

		/**
		 * Sets the name for this matching step.
		 *
		 * @param name The name of the matching step.
		 * @return This builder instance.
		 */
		public Builder<UA, CA> name(String name) {
			this.name = name;
			return this;
		}

		/**
		 * Builds an {@link AuthorMatcherStep} instance.
		 *
		 * @return A new instance of {@link AuthorMatcherStep}.
		 */
		public AuthorMatcherStep<UA, CA> build() {
			final BiFunction<UA, CA, Optional<AuthorMatch<UA, CA>>> matchingF = this.matchingFunc;
			final String stepName = name;

			return new AuthorMatcherStep<UA, CA>(
				(ua, ca) -> {
					AuthorMatch<UA, CA> res = matchingF.apply(ua, ca).orElse(null);
					if (res != null) {
						return Optional.of(res.withStepName(stepName));
					}
					return Optional.empty();
				},
				exclusionPredicate,
				stepName);
		}
	}
}
