
package eu.openaire.common.author;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AuthorMatchersTest {

	static List<AuthorMatch<TestAuthor, TestORCIDAuthor>> getMatches(List<TestAuthor> baseAuthors,
		List<TestORCIDAuthor> enrichingAuthors) {
		return AuthorMatchers
			.findMatches(
				baseAuthors,
				enrichingAuthors,
				Arrays
					.asList(
						AuthorMatcherStep
							.stringIgnoreCaseMatcher(TestAuthor::getFullName, TestORCIDAuthor::getFullName)
							.name("fullName")
							.build(),
						AuthorMatcherStep
							.stringIgnoreCaseMatcher(TestAuthor::getFullName, TestORCIDAuthor::getInvertedFullName)
							.name("invertedFullName")
							.build(),
						AuthorMatcherStep
							.abbreviationsMatcher(TestAuthor::getFullName, TestORCIDAuthor::getFullName)
							.name("orderedTokens")
							.build(),
						AuthorMatcherStep
							.stringIgnoreCaseMatcher(TestAuthor::getFullName, TestORCIDAuthor::getCreditName)
							.name("creditName")
							.build()));
	}

	@Test
	void basicTest() {
		// Data for DOI 10.1111/jbi.14978
		List<TestAuthor> authors = TestAuthor
			.of(
				"Marco Ferrante",
				"Gabor L. Lövei",
				"Andy G. Howe");

		List<TestORCIDAuthor> candidates = Arrays
			.asList(
				new TestORCIDAuthor("Marco", "Ferrante", "", "0000-0003-2421-396X"),
				new TestORCIDAuthor("Gabor", "Lövei", "", "0000-0002-6467-9812"),
				new TestORCIDAuthor("Andrew", "Howe", "Andy G. Howe", "0000-0002-7460-5227"));

		List<AuthorMatch<TestAuthor, TestORCIDAuthor>> result = getMatches(authors, candidates);

		for (AuthorMatch<TestAuthor, TestORCIDAuthor> match : result) {
			System.out.println(match);
		}
	}

	@Test
	void homonomyTest() {
		// Data for DOI https://doi.org/10.57805/revstat.v20i4.382
		List<TestAuthor> authors = TestAuthor
			.of(
				"Otto, Philipp",
				"Otto, P.");

		List<TestORCIDAuthor> candidates = Arrays
			.asList(
				new TestORCIDAuthor("Philipp", "Otto", "", "0000-0001-8630-108X"),
				new TestORCIDAuthor("Philipp", "Otto", "", "0000-0002-9796-6682"));

		List<AuthorMatch<TestAuthor, TestORCIDAuthor>> result = getMatches(authors, candidates);

		for (AuthorMatch<TestAuthor, TestORCIDAuthor> match : result) {
			System.out.println(match);
		}
	}

	@Test
	void accentInsensitiveTest() {
		// Data for DOI 10.48550/arxiv.1210.5363
		List<TestAuthor> authors = TestAuthor
			.of(
				"Michal Pilipczuk");

		List<TestORCIDAuthor> candidates = Arrays
			.asList(
				new TestORCIDAuthor("Michał", "Pilipczuk", "", "0000-0001-7891-1988"));
		List<AuthorMatch<TestAuthor, TestORCIDAuthor>> result = getMatches(authors, candidates);

		for (AuthorMatch<TestAuthor, TestORCIDAuthor> match : result) {
			System.out.println(match);
		}
	}
}
