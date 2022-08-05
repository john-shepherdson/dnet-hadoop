
package eu.dnetlib.dhp.common.vocabulary;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Maps;

import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

public class Vocabulary implements Serializable {

	private final String id;
	private final String name;

	/**
	 * Code to Term mappings for this Vocabulary.
	 */
	private final Map<String, VocabularyTerm> terms = new HashMap<>();

	/**
	 * Synonym to Code mappings for this Vocabulary.
	 */
	private final Map<String, String> synonyms = Maps.newHashMap();

	public Vocabulary(final String id, final String name) {
		this.id = id;
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	protected Map<String, VocabularyTerm> getTerms() {
		return terms;
	}

	public VocabularyTerm getTerm(final String id) {
		return Optional.ofNullable(id).map(String::toLowerCase).map(terms::get).orElse(null);
	}

	protected void addTerm(final String id, final String name) {
		terms.put(id.toLowerCase(), new VocabularyTerm(id, name));
	}

	protected boolean termExists(final String id) {
		return terms.containsKey(id.toLowerCase());
	}

	protected void addSynonym(final String syn, final String termCode) {
		synonyms.put(syn, termCode.toLowerCase());
	}

	public VocabularyTerm getTermBySynonym(final String syn) {
		return getTerm(synonyms.get(syn.toLowerCase()));
	}

	public Qualifier getTermAsQualifier(final String termId) {
		if (StringUtils.isBlank(termId)) {
			return OafMapperUtils.unknown(getId(), getName());
		} else if (termExists(termId)) {
			final VocabularyTerm t = getTerm(termId);
			return OafMapperUtils.qualifier(t.getId(), t.getName(), getId(), getName());
		} else {
			return OafMapperUtils.qualifier(termId, termId, getId(), getName());
		}
	}

	public Qualifier getSynonymAsQualifier(final String syn) {
		return Optional
			.ofNullable(getTermBySynonym(syn))
			.map(term -> getTermAsQualifier(term.getId()))
			.orElse(null);
	}

	public Qualifier lookup(String id) {
		return Optional
				.ofNullable(getSynonymAsQualifier(id))
				.orElse(getTermAsQualifier(id));
	}

}
