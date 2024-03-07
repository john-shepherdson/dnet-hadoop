
package eu.dnetlib.dhp.oa.graph.raw;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.dom4j.DocumentException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.oa.graph.clean.GraphCleaningFunctionsTest;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

@ExtendWith(MockitoExtension.class)
class GenerateEntitiesApplicationTest {

	@Mock
	private ISLookUpService isLookUpService;

	@Mock
	private VocabularyGroup vocs;

	@BeforeEach
	public void setUp() throws IOException, ISLookUpException {

		lenient().when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARIES_XQUERY)).thenReturn(vocs());
		lenient()
			.when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARY_SYNONYMS_XQUERY))
			.thenReturn(synonyms());

		vocs = VocabularyGroup.loadVocsFromIS(isLookUpService);
	}

	@Test
	void testMergeResult() throws IOException, DocumentException {
		Result publication = getResult("oaf_record.xml", Publication.class);
		Result dataset = getResult("odf_dataset.xml", Dataset.class);
		Result software = getResult("odf_software.xml", Software.class);
		Result orp = getResult("oaf_orp.xml", OtherResearchProduct.class);

		verifyMerge(publication, dataset, Dataset.class, ModelConstants.DATASET_RESULTTYPE_CLASSID);
		verifyMerge(dataset, publication, Dataset.class, ModelConstants.DATASET_RESULTTYPE_CLASSID);

		verifyMerge(publication, software, Publication.class, ModelConstants.PUBLICATION_RESULTTYPE_CLASSID);
		verifyMerge(software, publication, Publication.class, ModelConstants.PUBLICATION_RESULTTYPE_CLASSID);

		verifyMerge(publication, orp, Publication.class, ModelConstants.PUBLICATION_RESULTTYPE_CLASSID);
		verifyMerge(orp, publication, Publication.class, ModelConstants.PUBLICATION_RESULTTYPE_CLASSID);

		verifyMerge(dataset, software, Dataset.class, ModelConstants.DATASET_RESULTTYPE_CLASSID);
		verifyMerge(software, dataset, Dataset.class, ModelConstants.DATASET_RESULTTYPE_CLASSID);

		verifyMerge(dataset, orp, Dataset.class, ModelConstants.DATASET_RESULTTYPE_CLASSID);
		verifyMerge(orp, dataset, Dataset.class, ModelConstants.DATASET_RESULTTYPE_CLASSID);

		verifyMerge(software, orp, Software.class, ModelConstants.SOFTWARE_RESULTTYPE_CLASSID);
		verifyMerge(orp, software, Software.class, ModelConstants.SOFTWARE_RESULTTYPE_CLASSID);
	}

	protected <T extends Result> void verifyMerge(Result publication, Result dataset, Class<T> clazz,
		String resultType) {
		final Result merge = OafMapperUtils.mergeResults(publication, dataset);
		assertTrue(clazz.isAssignableFrom(merge.getClass()));
		assertEquals(resultType, merge.getResulttype().getClassid());
	}

	protected <T extends Result> Result getResult(String xmlFileName, Class<T> clazz)
		throws IOException, DocumentException {
		final String xml = IOUtils.toString(getClass().getResourceAsStream(xmlFileName));
		return new OdfToOafMapper(vocs, false, true)
			.processMdRecord(xml)
			.stream()
			.filter(s -> clazz.isAssignableFrom(s.getClass()))
			.map(s -> (Result) s)
			.findFirst()
			.get();
	}

	private List<String> vocs() throws IOException {
		return IOUtils
			.readLines(
				GenerateEntitiesApplicationTest.class.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/terms.txt"));
	}

	private List<String> synonyms() throws IOException {
		return IOUtils
			.readLines(
				GenerateEntitiesApplicationTest.class
					.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/clean/synonyms.txt"));
	}

}
