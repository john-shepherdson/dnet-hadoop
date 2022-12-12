
package eu.dnetlib.dhp.common.vocabulary;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
		return getTermAsQualifier(termId, false);
	}

	public Qualifier getTermAsQualifier(final String termId, boolean strict) {
		final VocabularyTerm term = getTerm(termId);
		if (Objects.nonNull(term)) {
			return OafMapperUtils.qualifier(term.getId(), term.getName(), getId(), getName());
		} else if (Objects.isNull(term) && strict) {
			return OafMapperUtils.unknown(getId(), getName());
		} else {
			return OafMapperUtils.qualifier(termId, termId, getId(), getName());
		}
	}

	public Qualifier getSynonymAsQualifier(final String syn) {
		return getSynonymAsQualifier(syn, false);
	}

	public Qualifier getSynonymAsQualifier(final String syn, boolean strict) {
		return Optional
			.ofNullable(getTermBySynonym(syn))
			.map(term -> getTermAsQualifier(term.getId(), strict))
			.orElse(null);
	}

	public Qualifier lookup(String id) {
		return lookup(id, false);
	}

	public Qualifier lookup(String id, boolean strict) {
		return Optional
			.ofNullable(getSynonymAsQualifier(id, strict))
			.orElse(getTermAsQualifier(id, strict));
	}

}
