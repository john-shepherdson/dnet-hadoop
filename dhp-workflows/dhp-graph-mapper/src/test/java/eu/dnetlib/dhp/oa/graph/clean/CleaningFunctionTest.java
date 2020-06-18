
package eu.dnetlib.dhp.oa.graph.clean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.graph.raw.common.VocabularyGroup;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
public class CleaningFunctionTest {

	public static final ObjectMapper MAPPER = new ObjectMapper();

	@Mock
	private ISLookUpService isLookUpService;

	private VocabularyGroup vocabularies;

	private CleaningRuleMap mapping;

	@BeforeEach
	public void setUp() throws ISLookUpException, IOException {
		lenient().when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARIES_XQUERY)).thenReturn(vocs());
		lenient()
			.when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARY_SYNONYMS_XQUERY))
			.thenReturn(synonyms());

		vocabularies = VocabularyGroup.loadVocsFromIS(isLookUpService);
		mapping = CleaningRuleMap.create(vocabularies);
	}

	@Test
	public void testCleaning() throws Exception {

		assertNotNull(vocabularies);
		assertNotNull(mapping);

		String json = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/result.json"));
		Publication p_in = MAPPER.readValue(json, Publication.class);

		assertTrue(p_in instanceof Result);
		assertTrue(p_in instanceof Publication);

		Publication p_out = OafCleaner.apply(p_in, mapping);

		assertNotNull(p_out);

		assertEquals("und", p_out.getLanguage().getClassid());
		assertEquals("Undetermined", p_out.getLanguage().getClassname());

		assertEquals("DE", p_out.getCountry().get(0).getClassid());
		assertEquals("Germany", p_out.getCountry().get(0).getClassname());

		assertEquals("0018", p_out.getInstance().get(0).getInstancetype().getClassid());
		assertEquals("Annotation", p_out.getInstance().get(0).getInstancetype().getClassname());

		assertEquals("CLOSED", p_out.getInstance().get(0).getAccessright().getClassid());
		assertEquals("Closed Access", p_out.getInstance().get(0).getAccessright().getClassname());

		Set<String> pidTerms = vocabularies.getTerms("dnet:pid_types");
		assertTrue(
			p_out
				.getPid()
				.stream()
				.map(p -> p.getQualifier())
				.allMatch(q -> pidTerms.contains(q.getClassid())));

		// TODO add more assertions to verity the cleaned values
		System.out.println(MAPPER.writeValueAsString(p_out));

		/*
		 * assertTrue( p_out .getPid() .stream() .allMatch(sp -> StringUtils.isNotBlank(sp.getValue())));
		 */
	}

	private Stream<Qualifier> getAuthorPidTypes(Publication pub) {
		return pub
			.getAuthor()
			.stream()
			.map(a -> a.getPid())
			.flatMap(p -> p.stream())
			.map(s -> s.getQualifier());
	}

	private List<String> vocs() throws IOException {
		return IOUtils
			.readLines(CleaningFunctionTest.class.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/terms.txt"));
	}

	private List<String> synonyms() throws IOException {
		return IOUtils
			.readLines(CleaningFunctionTest.class.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/synonyms.txt"));
	}
}
