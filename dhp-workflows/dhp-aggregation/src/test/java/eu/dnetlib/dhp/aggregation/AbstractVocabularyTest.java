
package eu.dnetlib.dhp.aggregation;

import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.mockito.Mock;

import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.transformation.TransformationFactory;
import eu.dnetlib.dhp.transformation.TransformationJobTest;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public abstract class AbstractVocabularyTest {

	@Mock
	protected ISLookUpService isLookUpService;

	protected VocabularyGroup vocabularies;

	public void setUpVocabulary() throws ISLookUpException, IOException {
		lenient().when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARIES_XQUERY)).thenReturn(vocs());

		lenient()
			.when(isLookUpService.quickSearchProfile(VocabularyGroup.VOCABULARY_SYNONYMS_XQUERY))
			.thenReturn(synonyms());
		vocabularies = VocabularyGroup.loadVocsFromIS(isLookUpService);
	}

	private static List<String> vocs() throws IOException {
		return IOUtils
			.readLines(TransformationJobTest.class.getResourceAsStream("/eu/dnetlib/dhp/transform/terms.txt"));
	}

	private static List<String> synonyms() throws IOException {
		return IOUtils
			.readLines(TransformationJobTest.class.getResourceAsStream("/eu/dnetlib/dhp/transform/synonyms.txt"));
	}

	protected void mockupTrasformationRule(final String trule, final String path) throws Exception {
		final String trValue = IOUtils.toString(this.getClass().getResourceAsStream(path));

		lenient()
			.when(isLookUpService.quickSearchProfile(String.format(TransformationFactory.TRULE_XQUERY, trule)))
			.thenReturn(Collections.singletonList(trValue));
	}
}
