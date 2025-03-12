
package eu.openaire.common.author;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

public class AuthorMatchersTest {

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

		List<AuthorMatch<TestAuthor, TestORCIDAuthor>> result = AuthorMatchers
			.findMatches(
				authors,
				candidates,
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

		for (AuthorMatch<TestAuthor, TestORCIDAuthor> match : result) {
			System.out.println(match);
		}
	}
}
