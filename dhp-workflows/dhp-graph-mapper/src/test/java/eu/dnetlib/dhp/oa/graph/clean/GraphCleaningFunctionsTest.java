
package eu.dnetlib.dhp.oa.graph.clean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.MappableBlock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.GraphCleaningFunctions;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
public class GraphCleaningFunctionsTest {

	public static final ObjectMapper MAPPER = new ObjectMapper()
		.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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
	void testCleanRelations() throws Exception {

		List<String> lines = IOUtils
			.readLines(getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/relation.json"));
		for (String json : lines) {
			Relation r_in = MAPPER.readValue(json, Relation.class);
			assertNotNull(r_in);

			assertFalse(vocabularies.getTerms(ModelConstants.DNET_RELATION_RELCLASS).contains(r_in.getRelClass()));

			Relation r_out = OafCleaner.apply(r_in, mapping);
			assertTrue(vocabularies.getTerms(ModelConstants.DNET_RELATION_RELCLASS).contains(r_out.getRelClass()));
			assertTrue(vocabularies.getTerms(ModelConstants.DNET_RELATION_SUBRELTYPE).contains(r_out.getSubRelType()));

			assertEquals("iis", r_out.getDataInfo().getProvenanceaction().getClassid());
			assertEquals("Inferred by OpenAIRE", r_out.getDataInfo().getProvenanceaction().getClassname());
		}
	}

	@Test
	void testFilter_false() throws Exception {

		assertNotNull(vocabularies);
		assertNotNull(mapping);

		String json = IOUtils
			.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/result_invisible.json"));
		Publication p_in = MAPPER.readValue(json, Publication.class);

		assertTrue(p_in instanceof Result);
		assertTrue(p_in instanceof Publication);

		assertEquals(false, GraphCleaningFunctions.filter(p_in));
	}

	@Test
	void testFilter_true() throws Exception {

		assertNotNull(vocabularies);
		assertNotNull(mapping);

		String json = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/result.json"));
		Publication p_in = MAPPER.readValue(json, Publication.class);

		assertTrue(p_in instanceof Result);
		assertTrue(p_in instanceof Publication);

		assertEquals(true, GraphCleaningFunctions.filter(p_in));
	}

	@Test
	void testFilter_missing_invisible() throws Exception {

		assertNotNull(vocabularies);
		assertNotNull(mapping);

		String json = IOUtils
			.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/result_missing_invisible.json"));
		Publication p_in = MAPPER.readValue(json, Publication.class);

		assertTrue(p_in instanceof Result);
		assertTrue(p_in instanceof Publication);

		assertEquals(true, GraphCleaningFunctions.filter(p_in));
	}

	@Test
	void testCleaning() throws Exception {

		assertNotNull(vocabularies);
		assertNotNull(mapping);

		String json = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/result.json"));
		Publication p_in = MAPPER.readValue(json, Publication.class);

		assertNull(p_in.getBestaccessright());

		assertTrue(p_in instanceof Result);
		assertTrue(p_in instanceof Publication);

		Publication p_out = OafCleaner.apply(GraphCleaningFunctions.fixVocabularyNames(p_in), mapping);

		assertNotNull(p_out);

		assertNotNull(p_out.getPublisher());
		assertNull(p_out.getPublisher().getValue());

		assertEquals("und", p_out.getLanguage().getClassid());
		assertEquals("Undetermined", p_out.getLanguage().getClassname());

		assertEquals("DE", p_out.getCountry().get(0).getClassid());
		assertEquals("Germany", p_out.getCountry().get(0).getClassname());

		assertEquals("0018", p_out.getInstance().get(0).getInstancetype().getClassid());
		assertEquals("Annotation", p_out.getInstance().get(0).getInstancetype().getClassname());

		assertEquals("CLOSED", p_out.getInstance().get(0).getAccessright().getClassid());
		assertEquals("Closed Access", p_out.getInstance().get(0).getAccessright().getClassname());

		Set<String> pidTerms = vocabularies.getTerms(ModelConstants.DNET_PID_TYPES);
		assertTrue(
			p_out
				.getPid()
				.stream()
				.map(StructuredProperty::getQualifier)
				.allMatch(q -> pidTerms.contains(q.getClassid())));

		List<Instance> poi = p_out.getInstance();
		assertNotNull(poi);
		assertEquals(1, poi.size());

		final Instance poii = poi.get(0);
		assertNotNull(poii);
		assertNotNull(poii.getPid());

		assertEquals(2, poii.getPid().size());

		assertTrue(
			poii.getPid().stream().anyMatch(s -> s.getValue().equals("10.1007/s109090161569x")));
		assertTrue(poii.getPid().stream().anyMatch(s -> s.getValue().equals("10.1008/abcd")));

		assertNotNull(poii.getAlternateIdentifier());
		assertEquals(2, poii.getAlternateIdentifier().size());

		assertTrue(
			poii
				.getAlternateIdentifier()
				.stream()
				.anyMatch(s -> s.getValue().equals("10.1007/s109090161569x")));
		assertTrue(
			poii
				.getAlternateIdentifier()
				.stream()
				.anyMatch(s -> s.getValue().equals("10.1009/qwerty")));

		assertEquals(5, p_out.getTitle().size());

		Publication p_cleaned = GraphCleaningFunctions.cleanup(p_out);

		assertEquals(3, p_cleaned.getTitle().size());

		List<String> titles = p_cleaned
			.getTitle()
			.stream()
			.map(StructuredProperty::getValue)
			.collect(Collectors.toList());
		assertTrue(titles.contains("omic"));
		assertTrue(
			titles.contains("Optical response of strained- and unstrained-silicon cold-electron bolometers test"));
		assertTrue(titles.contains("｢マキャベリ的知性と心の理論の進化論｣ リチャード・バーン， アンドリュー・ホワイトゥン 編／藤田和生， 山下博志， 友永雅巳 監訳"));

		assertEquals("CLOSED", p_cleaned.getBestaccessright().getClassid());
		assertNull(p_out.getPublisher());

		assertEquals("1970-10-07", p_cleaned.getDateofacceptance().getValue());

		final List<Instance> pci = p_cleaned.getInstance();
		assertNotNull(pci);
		assertEquals(1, pci.size());

		final Instance pcii = pci.get(0);
		assertNotNull(pcii);
		assertNotNull(pcii.getPid());

		assertEquals(2, pcii.getPid().size());

		assertTrue(
			pcii.getPid().stream().anyMatch(s -> s.getValue().equals("10.1007/s109090161569x")));
		assertTrue(pcii.getPid().stream().anyMatch(s -> s.getValue().equals("10.1008/abcd")));

		assertNotNull(pcii.getAlternateIdentifier());
		assertEquals(1, pcii.getAlternateIdentifier().size());
		assertTrue(
			pcii
				.getAlternateIdentifier()
				.stream()
				.anyMatch(s -> s.getValue().equals("10.1009/qwerty")));

		getAuthorPids(p_cleaned).forEach(pid -> {
			System.out
				.println(
					String
						.format(
							"%s [%s - %s]", pid.getValue(), pid.getQualifier().getClassid(),
							pid.getQualifier().getClassname()));
		});

		// TODO add more assertions to verity the cleaned values
		System.out.println(MAPPER.writeValueAsString(p_cleaned));
	}

	private Stream<Qualifier> getAuthorPidTypes(Result pub) {
		return pub
			.getAuthor()
			.stream()
			.map(Author::getPid)
			.flatMap(Collection::stream)
			.map(StructuredProperty::getQualifier);
	}

	private Stream<StructuredProperty> getAuthorPids(Result pub) {
		return pub
			.getAuthor()
			.stream()
			.map(Author::getPid)
			.flatMap(Collection::stream);
	}

	private List<String> vocs() throws IOException {
		return IOUtils
			.readLines(
				GraphCleaningFunctionsTest.class.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/terms.txt"));
	}

	private List<String> synonyms() throws IOException {
		return IOUtils
			.readLines(
				GraphCleaningFunctionsTest.class.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/synonyms.txt"));
	}

	@Test
	public void testCleanDoiBoost() throws IOException {
		String json = IOUtils
			.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/doiboostpub.json"));
		Publication p_in = MAPPER.readValue(json, Publication.class);
		Publication p_out = OafCleaner.apply(GraphCleaningFunctions.fixVocabularyNames(p_in), mapping);
		Publication cleaned = GraphCleaningFunctions.cleanup(p_out);

		Assertions.assertEquals(true, GraphCleaningFunctions.filter(cleaned));
	}

	@Test
	public void testCleanDoiBoost2() throws IOException {
		String json = IOUtils
			.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/doiboostpub2.json"));
		Publication p_in = MAPPER.readValue(json, Publication.class);
		Publication p_out = OafCleaner.apply(GraphCleaningFunctions.fixVocabularyNames(p_in), mapping);
		Publication cleaned = GraphCleaningFunctions.cleanup(p_out);

		Assertions.assertEquals(true, GraphCleaningFunctions.filter(cleaned));

	}
}
