
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
 * between a base author (BA) and an enriching author (EA).
 *
 * <p>This class encapsulates a matching function, an exclusion predicate, and a name for the
 * matching step. It allows defining different strategies for comparing authors, such as full name
 * matching and abbreviation-based matching.</p>
 *
 * @param <BA> The type containing the base author data.
 * @param <EA> The type containing the enriching author data.
 */
public class AuthorMatcherStep<BA, EA> {
	private final BiFunction<BA, EA, Optional<AuthorMatch<BA, EA>>> matchingFunc;
	private final Predicate<List<AuthorMatch<BA, EA>>> exclusionPredicate;
	private final String name;

	private AuthorMatcherStep(BiFunction<BA, EA, Optional<AuthorMatch<BA, EA>>> matchingFunc,
		Predicate<List<AuthorMatch<BA, EA>>> exclusionPredicate, String name) {
		this.matchingFunc = matchingFunc;
		this.exclusionPredicate = exclusionPredicate;
		this.name = name;
	}

	/**
	 * Creates a builder for a matching step that compares strings ignoring case.
	 *
	 * @param ex1  Function to extract the string from a base author.
	 * @param ex2  Function to extract the string from an enriching author.
	 * @param <BA> The type of the base author.
	 * @param <EA> The type of the enriching author.
	 * @return A builder to further configure the matching step.
	 */
	public static <BA, EA> Builder<BA, EA> stringIgnoreCaseMatcher(Function<BA, String> ex1, Function<EA, String> ex2) {
		return new Builder<BA, EA>()
			.matchingFunc((ua, ca) -> {
				String base = ex1.apply(ua);
				String enriching = ex2.apply(ca);

				if (base == null || enriching == null)
					return Optional.empty();
				else if (base.toLowerCase(Locale.ROOT).equals(enriching.toLowerCase(Locale.ROOT))) {
					return Optional.of(new AuthorMatch<>(ua, ca, "", 1));
				}
				return Optional.empty();
			});
	}

	/**
	 * Creates a builder for a matching step that compares names based on abbreviations.
	 *
	 * @param ex1  Function to extract the author name from a base author.
	 * @param ex2  Function to extract the author name from an enriching author.
	 * @param <BA> The type of the base author.
	 * @param <EA> The type of the enriching author.
	 * @return A builder to further configure the matching step.
	 */
	public static <BA, EA> Builder<BA, EA> abbreviationsMatcher(Function<BA, String> ex1, Function<EA, String> ex2) {
		return new Builder<BA, EA>()
			.name("abbreviations")
			.matchingFunc((ua, ca) -> {
				String base = ex1.apply(ua);
				String enriching = ex2.apply(ca);

				return matchOrderedTokenAndAbbreviations(base, enriching)
					.map(confidence -> new AuthorMatch<>(ua, ca, "", confidence));
			});
	}

	/**
	 * Gets the matching function used in this step.
	 *
	 * @return The matching function.
	 */
	public BiFunction<BA, EA, Optional<AuthorMatch<BA, EA>>> getMatchingFunc() {
		return matchingFunc;
	}

	/**
	 * Gets the exclusion predicate used in this step.
	 *
	 * @return The exclusion predicate.
	 */
	public Predicate<List<AuthorMatch<BA, EA>>> getExclusionPredicate() {
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
	 * @param <BA> The type of the base author.
	 * @param <EA> The type of the enriching author.
	 */
	public static class Builder<BA, EA> {
		private BiFunction<BA, EA, Optional<AuthorMatch<BA, EA>>> matchingFunc;
		private Predicate<List<AuthorMatch<BA, EA>>> exclusionPredicate;
		private String name;

		/**
		 * Sets the matching function for this builder.
		 *
		 * @param matchingFunc The matching function to use.
		 * @return This builder instance.
		 */
		public Builder<BA, EA> matchingFunc(BiFunction<BA, EA, Optional<AuthorMatch<BA, EA>>> matchingFunc) {
			this.matchingFunc = matchingFunc;
			return this;
		}

		/**
		 * Sets the exclusion predicate for this builder.
		 *
		 * @param exclusionPredicate The exclusion predicate to use.
		 * @return This builder instance.
		 */
		public Builder<BA, EA> exclusionPredicate(Predicate<List<AuthorMatch<BA, EA>>> exclusionPredicate) {
			this.exclusionPredicate = exclusionPredicate;
			return this;
		}

		/**
		 * Sets the name for this matching step.
		 *
		 * @param name The name of the matching step.
		 * @return This builder instance.
		 */
		public Builder<BA, EA> name(String name) {
			this.name = name;
			return this;
		}

		/**
		 * Builds an {@link AuthorMatcherStep} instance.
		 *
		 * @return A new instance of {@link AuthorMatcherStep}.
		 */
		public AuthorMatcherStep<BA, EA> build() {
			final BiFunction<BA, EA, Optional<AuthorMatch<BA, EA>>> matchingF = this.matchingFunc;
			final String stepName = name;

			return new AuthorMatcherStep<BA, EA>(
				(ua, ca) -> {
					AuthorMatch<BA, EA> res = matchingF.apply(ua, ca).orElse(null);
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
