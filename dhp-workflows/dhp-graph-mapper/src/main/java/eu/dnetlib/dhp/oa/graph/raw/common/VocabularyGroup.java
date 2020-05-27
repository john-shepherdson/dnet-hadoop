
package eu.dnetlib.dhp.oa.graph.raw.common;

import java.util.HashMap;
import java.util.Map;

import eu.dnetlib.dhp.schema.oaf.Qualifier;

public class VocabularyGroup {

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
		if (termExists(vocId, id)) {
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
