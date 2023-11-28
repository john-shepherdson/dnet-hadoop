
package eu.dnetlib.oa.merge;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.merge.AuthorMerger;
import eu.dnetlib.dhp.schema.oaf.Author;

public class AuthorMergerTest {


	@Test
	public void testEnrcichAuthor() throws Exception {
		final ObjectMapper mapper = new ObjectMapper();

		BufferedReader pr = new BufferedReader(new InputStreamReader(
				Objects.requireNonNull(AuthorMergerTest.class.getResourceAsStream("/eu/dnetlib/dhp/oa/merge/authors_publication_sample.json"))));
		BufferedReader or = new BufferedReader(new InputStreamReader(
				Objects.requireNonNull(AuthorMergerTest.class.getResourceAsStream("/eu/dnetlib/dhp/oa/merge/authors_orcid_sample.json"))));

		TypeReference<List<Author>> aclass = new TypeReference<List<Author>>() {
		};
		String pubLine;

		int i = 0;
		while ((pubLine = pr.readLine()) != null) {
			final String pubId = pubLine;
			final String MatchPidOrcid = or.readLine();
			final String pubOrcid = or.readLine();

			final String data = pr.readLine();

			if (StringUtils.isNotBlank(data)) {
				List<Author> publicationAuthors = mapper.readValue(data, aclass);
				List<Author> orcidAuthors = mapper.readValue(or.readLine(), aclass);
				System.out.printf("OAF ID = %s \n", pubId);
				System.out.printf("ORCID Intersected ID = %s \n", pubOrcid);
				System.out.printf("OAF Author Size = %d \n", publicationAuthors.size());
				System.out.printf("Oricd Author Size = %d \n", orcidAuthors.size());
				System.out.printf("Oricd Matched PID = %s \n", MatchPidOrcid);

				long originalAuthorWithPiD = publicationAuthors
					.stream()
					.filter(
						a -> a.getPid() != null && a
							.getPid()
							.stream()
							.anyMatch(
								p -> p.getQualifier() != null
									&& p.getQualifier().getClassid().toLowerCase().contains("orcid")))
					.count();
				long start = System.currentTimeMillis();

//                final List<Author> enrichedList = AuthorMerger.enrichOrcid(publicationAuthors, orcidAuthors);
				final List<Author> enrichedList = AuthorMerger.enrichOrcid2(publicationAuthors, orcidAuthors);

				long enrichedAuthorWithPid = enrichedList
					.stream()
					.filter(
						a -> a.getPid() != null && a
							.getPid()
							.stream()
							.anyMatch(
								p -> p.getQualifier() != null
									&& p.getQualifier().getClassid().toLowerCase().contains("orcid")))
					.count();

				long totalTime = (System.currentTimeMillis() - start) / 1000;
				System.out
					.printf(
						"Enriched authors in %d seconds from %d pid to %d pid \n", totalTime, originalAuthorWithPiD,
						enrichedAuthorWithPid);

				System.out.println("=================");
			}
		}
	}

	@Test
	public void checkSimilarityTest() {
		final Author left = new Author();
		left.setName("Anand");
		left.setSurname("Rachna");
		left.setFullname("Anand, Rachna");

		System.out.println(AuthorMerger.normalizeFullName(left.getFullname()));

		final Author right = new Author();
		right.setName("Rachna");
		right.setSurname("Anand");
		right.setFullname("Rachna, Anand");
//        System.out.println(AuthorMerger.normalize(right.getFullname()));
		boolean same = AuthorMerger.checkSimilarity2(left, right);

		assertTrue(same);

	}

}
