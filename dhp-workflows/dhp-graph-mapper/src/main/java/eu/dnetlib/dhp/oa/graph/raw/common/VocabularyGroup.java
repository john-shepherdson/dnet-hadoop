
package eu.dnetlib.dhp.oa.graph.raw.common;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.schema.oaf.OafMapperUtils;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class VocabularyGroup implements Serializable {

	public static final String VOCABULARIES_XQUERY = "for $x in collection('/db/DRIVER/VocabularyDSResources/VocabularyDSResourceType') \n"
		+
		"let $vocid := $x//VOCABULARY_NAME/@code\n" +
		"let $vocname := $x//VOCABULARY_NAME/text()\n" +
		"for $term in ($x//TERM)\n" +
		"return concat($vocid,' @=@ ',$vocname,' @=@ ',$term/@code,' @=@ ',$term/@english_name)";

	public static final String VOCABULARY_SYNONYMS_XQUERY = "for $x in collection('/db/DRIVER/VocabularyDSResources/VocabularyDSResourceType')\n"
		+
		"let $vocid := $x//VOCABULARY_NAME/@code\n" +
		"let $vocname := $x//VOCABULARY_NAME/text()\n" +
		"for $term in ($x//TERM)\n" +
		"for $syn in ($term//SYNONYM/@term)\n" +
		"return concat($vocid,' @=@ ',$term/@code,' @=@ ', $syn)\n";

	public static VocabularyGroup loadVocsFromIS(ISLookUpService isLookUpService) throws ISLookUpException {

		final VocabularyGroup vocs = new VocabularyGroup();

		for (final String s : isLookUpService.quickSearchProfile(VOCABULARIES_XQUERY)) {
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
				// vocs.addSynonyms(vocId, termId, termId);
			}
		}

		for (final String s : isLookUpService.quickSearchProfile(VOCABULARY_SYNONYMS_XQUERY)) {
			final String[] arr = s.split("@=@");
			if (arr.length == 3) {
				final String vocId = arr[0].trim();
				final String termId = arr[1].trim();
				final String syn = arr[2].trim();

				vocs.addSynonyms(vocId, termId, syn);
				// vocs.addSynonyms(vocId, termId, termId);
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

	public Set<String> getTerms(String vocId) {
		if (!vocabularyExists(vocId)) {
			return new HashSet<>();
		}
		return vocs
			.get(vocId.toLowerCase())
			.getTerms()
			.values()
			.stream()
			.map(t -> t.getId())
			.collect(Collectors.toCollection(HashSet::new));
	}

	public Qualifier lookup(String vocId, String id) {
		return Optional
			.ofNullable(getSynonymAsQualifier(vocId, id))
			.orElse(getTermAsQualifier(vocId, id));
	}

	public Qualifier getTermAsQualifier(final String vocId, final String id) {
		if (vocabularyExists(vocId)) {
			return vocs.get(vocId.toLowerCase()).getTermAsQualifier(id);
		}
		return OafMapperUtils.qualifier(id, id, "", "");
	}

	public Qualifier getSynonymAsQualifier(final String vocId, final String syn) {
		if (StringUtils.isBlank(vocId)) {
			return OafMapperUtils.unknown("", "");
		}
		return vocs.get(vocId.toLowerCase()).getSynonymAsQualifier(syn);
	}

	public boolean termExists(final String vocId, final String id) {
		return vocabularyExists(vocId) && vocs.get(vocId.toLowerCase()).termExists(id);
	}

	public boolean vocabularyExists(final String vocId) {
		return Optional
			.ofNullable(vocId)
			.map(String::toLowerCase)
			.map(id -> vocs.containsKey(id))
			.orElse(false);
	}

	private void addSynonyms(final String vocId, final String termId, final String syn) {
		String id = Optional
			.ofNullable(vocId)
			.map(s -> s.toLowerCase())
			.orElseThrow(
				() -> new IllegalArgumentException(String.format("empty vocabulary id for [term:%s, synonym:%s]")));
		Optional
			.ofNullable(vocs.get(id))
			.orElseThrow(() -> new IllegalArgumentException("missing vocabulary id: " + vocId))
			.addSynonym(syn.toLowerCase(), termId);
	}

}
