
package eu.dnetlib.dhp.enrich.orcid;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Author;

public class ORCIDAuthorEnricherTest {

	@Test
	public void testEnrcichAuthor() throws Exception {
		final ObjectMapper mapper = new ObjectMapper();

		BufferedReader pr = new BufferedReader(new InputStreamReader(
			Objects
				.requireNonNull(
					ORCIDAuthorEnricherTest.class
						.getResourceAsStream("/eu/dnetlib/dhp/enrich/orcid/authors_publication_sample.json"))));
		BufferedReader or = new BufferedReader(new InputStreamReader(
			Objects
				.requireNonNull(
					ORCIDAuthorEnricherTest.class
						.getResourceAsStream("/eu/dnetlib/dhp/enrich/orcid/authors_orcid_sample.json"))));

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
				final List<Author> enrichedList = Collections.emptyList(); // SparkEnrichGraphWithOrcidAuthors.enrichOrcid(publicationAuthors,
																			// orcidAuthors);

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

}
