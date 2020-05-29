
package eu.dnetlib.dhp.oa.graph.raw.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.oa.graph.raw.GenerateEntitiesApplication;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class VocabularyGroup {

	public static VocabularyGroup loadVocsFromIS(final String isLookupUrl) throws IOException, ISLookUpException {
		final ISLookUpService isLookUpService = ISLookupClientFactory.getLookUpService(isLookupUrl);

		final String xquery = IOUtils
			.toString(
				GenerateEntitiesApplication.class
					.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/xquery/load_vocabularies.xquery"));

		final VocabularyGroup vocs = new VocabularyGroup();

		for (final String s : isLookUpService.quickSearchProfile(xquery)) {
			final String[] arr = s.split("@=@");
			if (arr.length == 4) {
				final String vocId = arr[0].trim();
				final String vocName = arr[1].trim();
				final String termId = arr[2].trim();
				final String termName = arr[3].trim();

				if (!vocs.vocabularyExists(vocId)) {
					vocs.addVocabulary(vocId, vocName);
				}

				vocs.addTerm(vocId, termId, termName);
			}
		}

		return vocs;
	}

	private final Map<String, Vocabulary> vocs = new HashMap<>();

	public void addVocabulary(final String id, final String name) {
		vocs.put(id.toLowerCase(), new Vocabulary(id, name));
	}

	public void addTerm(final String vocId, final String id, final String name) {
		if (vocabularyExists(vocId)) {
			vocs.get(vocId.toLowerCase()).addTerm(id, name);
		}
	}

	public VocabularyTerm getTerm(final String vocId, final String id) {
		if (termExists(vocId, id)) {
			return vocs.get(vocId.toLowerCase()).getTerm(id);
		} else {
			return new VocabularyTerm(id, id);
		}
	}

	public Qualifier getTermAsQualifier(final String vocId, final String id) {
		if (StringUtils.isBlank(id)) {
			return OafMapperUtils.qualifier("UNKNOWN", "UNKNOWN", vocId, vocId);
		} else if (termExists(vocId, id)) {
			final Vocabulary v = vocs.get(vocId.toLowerCase());
			final VocabularyTerm t = v.getTerm(id);
			return OafMapperUtils.qualifier(t.getId(), t.getName(), v.getId(), v.getName());
		} else {
			return OafMapperUtils.qualifier(id, id, vocId, vocId);
		}
	}

	public boolean termExists(final String vocId, final String id) {
		return vocabularyExists(vocId) && vocs.get(vocId.toLowerCase()).termExists(id);
	}

	public boolean vocabularyExists(final String vocId) {
		return vocs.containsKey(vocId.toLowerCase());
	}

}
