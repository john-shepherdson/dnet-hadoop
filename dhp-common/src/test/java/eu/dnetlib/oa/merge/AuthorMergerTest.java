
package eu.dnetlib.oa.merge;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.merge.AuthorMerger;
import eu.dnetlib.dhp.schema.oaf.Author;

public class AuthorMergerTest {

	@Test
	public void testNormalization() {

		assertEquals("bruzzolasandro", AuthorMerger.normalizeFullName("Sandro, La Bruzzo"));
		assertEquals("baglionimiriam", AuthorMerger.normalizeFullName("Miriam Baglioni"));
		assertEquals("baglionimiriam", AuthorMerger.normalizeFullName("Miriam ;Baglioni,"));

	}

	public void testEnrcichAuthor() throws Exception {
		final ObjectMapper mapper = new ObjectMapper();

		BufferedReader pr = new BufferedReader(new InputStreamReader(
			AuthorMergerTest.class.getResourceAsStream("/eu/dnetlib/dhp/oa/merge/authors_publication.json")));
		BufferedReader or = new BufferedReader(new InputStreamReader(
			AuthorMergerTest.class.getResourceAsStream("/eu/dnetlib/dhp/oa/merge/authors_orcid.json")));

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

				if (++i > 30)
					break;
			}

		}

	}

	@Test
	public void checkSimilarityTest() {
		final Author left = new Author();
		left.setSurname("Wu");
		left.setName("M.");
		left.setFullname("Wu, M.");

		System.out.println(AuthorMerger.normalizeFullName(left.getFullname()));

		final Author right = new Author();
		right.setName("Xin");
		right.setSurname("Wu");
		right.setFullname("Xin Wu");
//        System.out.println(AuthorMerger.normalize(right.getFullname()));
		boolean same = AuthorMerger.checkSimilarity2(left, right);

		assertFalse(same);

	}

}
