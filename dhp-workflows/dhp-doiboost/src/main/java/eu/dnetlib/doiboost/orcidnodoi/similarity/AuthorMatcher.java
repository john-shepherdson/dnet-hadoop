
package eu.dnetlib.doiboost.orcidnodoi.similarity;

import java.io.IOException;
import java.text.Normalizer;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ximpleware.NavException;
import com.ximpleware.ParseException;
import com.ximpleware.XPathEvalException;
import com.ximpleware.XPathParseException;

import eu.dnetlib.dhp.parser.utility.VtdException;
import eu.dnetlib.dhp.schema.orcid.AuthorData;
import eu.dnetlib.dhp.schema.orcid.Contributor;
import eu.dnetlib.dhp.schema.orcid.WorkDetail;

/**
 * This class is used for searching from a list of publication contributors a
 * specific author making a similarity check on both name and surname of the
 * author with the credit name of each contributor of the list; as soon as
 * the match is found (if exist) author informations are used to enrich the
 * matched contribuotr inside contributors list
 */

public class AuthorMatcher {

	private static final Logger logger = LoggerFactory.getLogger(AuthorMatcher.class);
	public static final Double threshold = 0.8;

	public static void match(AuthorData author, List<Contributor> contributors)
		throws IOException, XPathEvalException, XPathParseException, NavException, VtdException, ParseException {

		int matchCounter = 0;
		List<Integer> matchCounters = Arrays.asList(matchCounter);
		Contributor contributor = null;
		contributors
			.stream()
			.filter(c -> !StringUtils.isBlank(c.getCreditName()))
			.forEach(c -> {
				if (simpleMatch(c.getCreditName(), author.getName()) ||
					simpleMatch(c.getCreditName(), author.getSurname()) ||
					simpleMatchOnOtherNames(c.getCreditName(), author.getOtherNames())) {
					matchCounters.set(0, matchCounters.get(0) + 1);
					c.setSimpleMatch(true);
				}
			});
		if (matchCounters.get(0) == 1) {
			updateAuthorsSimpleMatch(contributors, author);
		} else if (matchCounters.get(0) == 0) {
			Optional<Contributor> optCon = contributors
				.stream()
				.filter(c -> !StringUtils.isBlank(c.getCreditName()))
				.map(c -> {
					c.setScore(bestMatch(author.getName(), author.getSurname(), c.getCreditName()));
					return c;
				})
				.filter(c -> c.getScore() >= threshold)
				.max(Comparator.comparing(c -> c.getScore()));
			Contributor bestMatchContributor = null;
			if (optCon.isPresent()) {
				bestMatchContributor = optCon.get();
				bestMatchContributor.setBestMatch(true);
				updateAuthorsSimilarityMatch(contributors, author);
			}
		} else if (matchCounters.get(0) > 1) {
			Optional<Contributor> optCon = contributors
				.stream()
				.filter(c -> c.isSimpleMatch())
				.filter(c -> !StringUtils.isBlank(c.getCreditName()))
				.map(c -> {
					c.setScore(bestMatch(author.getName(), author.getSurname(), c.getCreditName()));
					return c;
				})
				.filter(c -> c.getScore() >= threshold)
				.max(Comparator.comparing(c -> c.getScore()));
			Contributor bestMatchContributor = null;
			if (optCon.isPresent()) {
				bestMatchContributor = optCon.get();
				bestMatchContributor.setBestMatch(true);
				updateAuthorsSimilarityMatch(contributors, author);
			}
		}

	}

	public static boolean simpleMatchOnOtherNames(String name, List<String> otherNames) {
		if (otherNames == null || (otherNames != null && otherNames.isEmpty())) {
			return false;
		}
		return otherNames.stream().filter(o -> simpleMatch(name, o)).count() > 0;
	}

	public static boolean simpleMatch(String name, String searchValue) {
		if (searchValue == null) {
			return false;
		}
		return normalize(name).contains(normalize(searchValue));
	}

	public static Double bestMatch(String authorSurname, String authorName, String contributor) {
		String[] contributorSplitted = contributor.split(" ");
		if (contributorSplitted.length == 0) {
			return 0.0;
		}
		final String contributorName = contributorSplitted[contributorSplitted.length - 1];
		String contributorSurname = "";
		if (contributorSplitted.length > 1) {
			StringJoiner joiner = new StringJoiner(" ");
			for (int i = 0; i < contributorSplitted.length - 1; i++) {
				joiner.add(contributorSplitted[i]);
			}
			contributorSurname = joiner.toString();
		}
		String authorNameNrm = normalize(authorName);
		String authorSurnameNrm = normalize(authorSurname);
		String contributorNameNrm = normalize(contributorName);
		String contributorSurnameNrm = normalize(contributorSurname);
		Double sm1 = similarity(authorNameNrm, authorSurnameNrm, contributorNameNrm, contributorSurnameNrm);
		Double sm2 = similarity(authorNameNrm, authorSurnameNrm, contributorSurnameNrm, contributorNameNrm);
		if (sm1.compareTo(sm2) >= 0) {
			return sm1;
		}
		return sm2;
	}

	public static Double similarity(String nameA, String surnameA, String nameB, String surnameB) {
		Double score = similarityJaroWinkler(nameA, surnameA, nameB, surnameB);
		return score;
	}

	private static Double similarityJaroWinkler(String nameA, String surnameA, String nameB, String surnameB) {
		return new JaroWinklerSimilarity().apply(normalize(parse(nameA, surnameA)), normalize(parse(nameB, surnameB)));
	}

	public static String normalize(final String s) {
		if (s == null) {
			return new String("");
		}
		return nfd(s)
			.toLowerCase()
			// do not compact the regexes in a single expression, would cause StackOverflowError
			// in case
			// of large input strings
			.replaceAll("(\\W)+", " ")
			.replaceAll("(\\p{InCombiningDiacriticalMarks})+", " ")
			.replaceAll("(\\p{Punct})+", " ")
			.replaceAll("(\\d)+", " ")
			.replaceAll("(\\n)+", " ")
			.trim();
	}

	private static String nfd(final String s) {
		return Normalizer.normalize(s, Normalizer.Form.NFD);
	}

	private static String parse(String name, String surname) {
		return surname + " " + name;
	}

	public static void updateAuthorsSimpleMatch(List<Contributor> contributors, AuthorData author) {
		contributors.forEach(c -> {
			if (c.isSimpleMatch()) {
				c.setName(author.getName());
				c.setSurname(author.getSurname());
				c.setOid(author.getOid());
			}
		});
		updateRanks(contributors);
	}

	public static void updateAuthorsSimilarityMatch(List<Contributor> contributors, AuthorData author) {
		contributors
			.stream()
			.filter(c -> c.isBestMatch())
			.forEach(c -> {
				c.setName(author.getName());
				c.setSurname(author.getSurname());
				c.setOid(author.getOid());
			});
		updateRanks(contributors);
	}

	private static void updateRanks(List<Contributor> contributors) {
		boolean seqFound = false;
		if (contributors
			.stream()
			.filter(
				c -> c.getRole() != null && c.getSequence() != null &&
					c.getRole().equals("author") && (c.getSequence().equals("first") ||
						c.getSequence().equals("additional")))
			.count() > 0) {
			seqFound = true;
		}
		if (!seqFound) {
			List<Integer> seqIds = Arrays.asList(0);
			contributors.forEach(c -> {
				int currentSeq = seqIds.get(0) + 1;
				seqIds.set(0, currentSeq);
				c.setSequence(Integer.toString(seqIds.get(0)));
			});
		}
	}

	private static String toJson(WorkDetail work) {
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		return gson.toJson(work);
	}
}
