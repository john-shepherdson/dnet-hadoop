
package eu.dnetlib.dhp.common.vocabulary;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
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
			}
		}

		for (final String s : isLookUpService.quickSearchProfile(VOCABULARY_SYNONYMS_XQUERY)) {
			final String[] arr = s.split("@=@");
			if (arr.length == 3) {
				final String vocId = arr[0].trim();
				final String termId = arr[1].trim();
				final String syn = arr[2].trim();

				vocs.addSynonyms(vocId, termId, syn);

			}
		}

		// add the term names as synonyms
		vocs.vocs.values().forEach(voc -> {
			voc.getTerms().values().forEach(term -> {
				voc.addSynonym(term.getName().toLowerCase(), term.getId());
			});
		});

		return vocs;
	}

	private final Map<String, Vocabulary> vocs = new HashMap<>();

	public Set<String> vocabularyNames() {
		return vocs.keySet();
	}

	public void addVocabulary(final String id, final String name) {
		vocs.put(id.toLowerCase(), new Vocabulary(id, name));
	}

	public Optional<Vocabulary> find(final String vocId) {
		return Optional
			.ofNullable(vocId)
			.map(String::toLowerCase)
			.map(vocs::get);
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
			.map(VocabularyTerm::getId)
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

	public Qualifier lookupTermBySynonym(final String vocId, final String syn) {
		if (StringUtils.isBlank(vocId)) {
			return OafMapperUtils.unknown("", "");
		}

		final Vocabulary vocabulary = vocs.get(vocId.toLowerCase());

		return Optional
			.ofNullable(vocabulary.getTerm(syn))
			.map(
				term -> OafMapperUtils
					.qualifier(term.getId(), term.getName(), vocabulary.getId(), vocabulary.getName()))
			.orElse(
				Optional
					.ofNullable(vocabulary.getTermBySynonym(syn))
					.map(
						term -> OafMapperUtils
							.qualifier(term.getId(), term.getName(), vocabulary.getId(), vocabulary.getName()))
					.orElse(null));
	}

	/**
	 * getSynonymAsQualifierCaseSensitive
	 *
	 * refelects the situation to check caseSensitive vocabulary
	 */
	public Qualifier getSynonymAsQualifierCaseSensitive(final String vocId, final String syn) {
		if (StringUtils.isBlank(vocId)) {
			return OafMapperUtils.unknown("", "");
		}
		return vocs.get(vocId).getSynonymAsQualifier(syn);
	}

	/**
	 * termExists
	 *
	 * two methods: without and with caseSensitive check
	 */
	public boolean termExists(final String vocId, final String id) {
		return termExists(vocId, id, Boolean.FALSE);
	}

	public boolean termExists(final String vocId, final String id, final Boolean caseSensitive) {
		if (Boolean.TRUE.equals(caseSensitive)) {
			return vocabularyExists(vocId) && vocs.get(vocId).termExists(id);
		}
		return vocabularyExists(vocId) && vocs.get(vocId.toLowerCase()).termExists(id);
	}

	public boolean vocabularyExists(final String vocId) {
		return Optional
			.ofNullable(vocId)
			.map(String::toLowerCase)
			.map(vocs::containsKey)
			.orElse(false);
	}

	private void addSynonyms(final String vocId, final String termId, final String syn) {
		String id = Optional
			.ofNullable(vocId)
			.map(String::toLowerCase)
			.orElseThrow(
				() -> new IllegalArgumentException(
					String
						.format(
							"empty vocabulary id for [term:%s, synonym:%s]", termId, syn)));
		Optional
			.ofNullable(vocs.get(id))
			.orElseThrow(() -> new IllegalArgumentException("missing vocabulary id: " + vocId))
			.addSynonym(syn.toLowerCase(), termId);
	}

}
