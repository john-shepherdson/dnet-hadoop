
package eu.dnetlib.doiboost.orcidnodoi.xml;

import com.ximpleware.NavException;
import com.ximpleware.ParseException;
import com.ximpleware.XPathEvalException;
import com.ximpleware.XPathParseException;
import eu.dnetlib.dhp.parser.utility.VtdException;
import eu.dnetlib.doiboost.orcid.model.AuthorData;
import eu.dnetlib.doiboost.orcidnodoi.model.Contributor;
import eu.dnetlib.doiboost.orcidnodoi.model.WorkDataNoDoi;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.similarity.JaccardSimilarity;
import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.Normalizer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class OrcidNoDoiTest {

	private static final Logger logger = LoggerFactory.getLogger(OrcidNoDoiTest.class);

	String nameA = "Khairy";
	String surnameA = "Abdel Dayem";
	String otherNameA = "Dayem MKA";
	String nameB = "K";
	String surnameB = "Abdel-Dayem";
	String orcidIdA = "0000-0003-2760-1191";
	Double threshold = 0.8;

	@Test
	@Ignore
	private void similarityTest() throws Exception {
		logger.info("running testSimilarity ....");
		logger
			.info(
				"JaroWinklerSimilarity: "
					+ Double.toString(similarityJaroWinkler(nameA, surnameA, nameB, surnameB)));
		logger
			.info(
				"JaccardSimilarity: " + Double.toString(similarityJaccard(nameA, surnameA, nameB, surnameB)));
	}

	@Test
	@Ignore
	private void bestMatchTest() throws Exception {
		logger.info("running bestMatchTest ....");
		String contributor = surnameB + ", " + nameB;
		logger.info("score: " + Double.toString(bestMatch(surnameA, nameA, contributor)));
	}

	private static Double bestMatch(String authorSurname, String authorName, String contributor) {
		logger.debug(authorSurname + " " + authorName + " vs " + contributor);
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
		logger
			.debug(
				"contributorName: " + contributorName +
					" contributorSurname: " + contributorSurname);
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

	private static Double similarity(String nameA, String surnameA, String nameB, String surnameB) {
		Double score = similarityJaroWinkler(nameA, surnameA, nameB, surnameB);
		logger
			.debug(nameA + ", " + surnameA + " <> " + nameB + ", " + surnameB + "   score: " + Double.toString(score));
		return score;
	}

	private static Double similarityJaccard(String nameA, String surnameA, String nameB, String surnameB) {
		return new JaccardSimilarity().apply(normalize(parse(nameA, surnameA)), normalize(parse(nameB, surnameB)));
	}

	private static Double similarityJaroWinkler(String nameA, String surnameA, String nameB, String surnameB) {
		return new JaroWinklerSimilarity().apply(normalize(parse(nameA, surnameA)), normalize(parse(nameB, surnameB)));
	}

	private static String parse(String name, String surname) {
		return surname + " " + name;
	}

	private static String normalize(final String s) {
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

	@Test
	@Ignore
	public void readPublicationFieldsTest()
		throws IOException, XPathEvalException, XPathParseException, NavException, VtdException, ParseException {
		logger.info("running loadPublicationFieldsTest ....");
		String xml = IOUtils
			.toString(
				OrcidNoDoiTest.class.getResourceAsStream("activity_work_0000-0003-2760-1191.xml"));

		if (xml == null) {
			logger.info("Resource not found");
		}
		XMLRecordParserNoDoi p = new XMLRecordParserNoDoi();
		if (p == null) {
			logger.info("XMLRecordParserNoDoi null");
		}
		WorkDataNoDoi workData = null;
		try {
			workData = p.VTDParseWorkData(xml.getBytes());
		} catch (Exception e) {
			logger.error("parsing xml", e);
		}
		assertNotNull(workData);
		assertNotNull(workData.getOid());
		logger.info("oid: " + workData.getOid());
		assertNotNull(workData.getTitles());
		logger.info("titles: ");
		workData.getTitles().forEach(t -> {
			logger.info(t);
		});
		logger.info("source: " + workData.getSourceName());
		logger.info("type: " + workData.getType());
		logger.info("urls: ");
		workData.getUrls().forEach(u -> {
			logger.info(u);
		});
		logger.info("publication date: ");
		workData.getPublicationDates().forEach(d -> {
			logger.info(d.getYear() + " - " + d.getMonth() + " - " + d.getDay());
		});
		logger.info("external id: ");
		workData.getExtIds().removeIf(e -> e.getRelationShip() != null && !e.getRelationShip().equals("self"));
		workData.getExtIds().forEach(e -> {
			logger.info(e.getType() + " - " + e.getValue() + " - " + e.getRelationShip());
		});
		logger.info("contributors: ");
		workData.getContributors().forEach(c -> {
			logger
				.info(
					c.getName() + " - " + c.getRole() + " - " + c.getSequence());
		});

	}

	private void updateRanks(List<Contributor> contributors) {
		boolean seqFound = false;
		if (contributors
			.stream()
			.filter(
				c -> c.getRole() != null && c.getSequence() != null &&
					c.getRole().equals("author") && (c.getSequence().equals("first") ||
						c.getSequence().equals("additional")))
			.count() > 0) {
			seqFound = true;
			logger.info("sequence data found");
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

	private void updateAuthorsSimpleMatch(List<Contributor> contributors, AuthorData author) {
		contributors.forEach(c -> {
			if (c.isSimpleMatch()) {
				logger.info("simple match on : " + c.getCreditName());
				c.setName(author.getName());
				c.setSurname(author.getSurname());
				c.setOid(author.getOid());
			}
		});
		updateRanks(contributors);
	}

	private void updateAuthorsSimilarityMatch(List<Contributor> contributors, AuthorData author) {
		logger.info("inside updateAuthorsSimilarityMatch ...");
		contributors.forEach(c -> {
			logger
				.info(
					c.getOid() + " - " + c.getCreditName() + " - " +
						c.getName() + " - " + c.getSurname() + " - " +
						c.getRole() + " - " + c.getSequence() + " - best: " + c.isBestMatch() + " - simpe: "
						+ c.isSimpleMatch());
		});

		contributors
			.stream()
			.filter(c -> c.isBestMatch())
			.forEach(c -> {
				logger.info("similarity match on : " + c.getCreditName());
				c.setName(author.getName());
				c.setSurname(author.getSurname());
				c.setOid(author.getOid());
			});
		updateRanks(contributors);
	}

	@Test
	@Ignore
	public void authorSimilarityMatchTest() throws Exception {
		logger.info("running authorSimilarityMatchTest ....");
		authorMatchTest("activity_work_0000-0003-2760-1191-similarity.xml");
	}

	@Test
	private void authorSimpleMatchTest() throws Exception {
		logger.info("running authorSimpleMatchTest ....");
		authorMatchTest("activity_work_0000-0003-2760-1191.xml");
	}

	private void authorMatchTest(String orcidWork)
		throws IOException, XPathEvalException, XPathParseException, NavException, VtdException, ParseException {
		AuthorData author = new AuthorData();
		author.setName(nameA);
		author.setSurname(surnameA);
		author.setOid(orcidIdA);
		String xml = IOUtils
			.toString(
				OrcidNoDoiTest.class.getResourceAsStream(orcidWork));

		if (xml == null) {
			logger.info("Resource not found");
		}
		XMLRecordParserNoDoi p = new XMLRecordParserNoDoi();
		if (p == null) {
			logger.info("XMLRecordParserNoDoi null");
		}
		WorkDataNoDoi workData = null;
		try {
			workData = p.VTDParseWorkData(xml.getBytes());
		} catch (Exception e) {
			logger.error("parsing xml", e);
		}
		assertNotNull(workData);
		int matchCounter = 0;
		List<Integer> matchCounters = Arrays.asList(matchCounter);
		Contributor contributor = null;
		workData.getContributors().forEach(c -> {
			if (normalize(c.getCreditName()).contains(normalize(author.getName())) ||
				normalize(c.getCreditName()).contains(normalize(author.getSurname())) ||
				((author.getOtherName() != null)
					&& normalize(c.getCreditName()).contains(normalize(author.getOtherName())))) {
				matchCounters.set(0, matchCounters.get(0) + 1);
				c.setSimpleMatch(true);
			}
		});
		logger.info("match counter: " + Integer.toString(matchCounters.get(0)));
		if (matchCounters.get(0) == 1) {
			updateAuthorsSimpleMatch(workData.getContributors(), author);
		} else if (matchCounters.get(0) > 1) {
			Optional<Contributor> optCon = workData
				.getContributors()
				.stream()
				.filter(c -> c.isSimpleMatch())
				.map(c -> {
					c.setScore(bestMatch(nameA, surnameA, c.getCreditName()));
					logger.debug("nella map: " + c.getCreditName() + " score: " + c.getScore());
					return c;
				})
				.filter(c -> c.getScore() >= threshold)
				.max(Comparator.comparing(c -> c.getScore()));
			Contributor bestMatchContributor = null;
			if (optCon.isPresent()) {
				bestMatchContributor = optCon.get();
				bestMatchContributor.setBestMatch(true);
				logger.info("best match: " + bestMatchContributor.getCreditName());
				updateAuthorsSimilarityMatch(workData.getContributors(), author);
			}

		}

		logger.info("UPDATED contributors: ");
		workData.getContributors().forEach(c -> {
			logger
				.info(
					c.getOid() + " - " + c.getCreditName() + " - " +
						c.getName() + " - " + c.getSurname() + " - " +
						c.getRole() + " - " + c.getSequence());
		});
	}
}

//
//		orcid_RDD = sc.textFile(ORCID_DUMP_PATH)
//		no_doi_works_RDD = orcid_RDD.map(orcid_map).filter(lambda x:x is not None).map(lambda x: json.dumps(x)).saveAsTextFile(path=ORCID_OPENAIRE_PATH,compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
//