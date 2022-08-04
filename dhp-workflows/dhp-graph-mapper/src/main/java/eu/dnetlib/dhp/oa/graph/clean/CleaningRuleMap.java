
package eu.dnetlib.dhp.oa.graph.clean;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.dhp.common.FunctionalInterfaceSupport.SerializableConsumer;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.AccessRight;
import eu.dnetlib.dhp.schema.oaf.Country;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class CleaningRuleMap extends HashMap<Class<?>, SerializableConsumer<Object>> implements Serializable {

	/**
	 * Creates the mapping for the Oaf types subject to cleaning
	 *
	 * @param vocabularies
	 */
	public static CleaningRuleMap create(VocabularyGroup vocabularies) {
		CleaningRuleMap mapping = new CleaningRuleMap();
		mapping.put(Qualifier.class, o -> cleanQualifier(vocabularies, (Qualifier) o));
		mapping.put(AccessRight.class, o -> cleanQualifier(vocabularies, (AccessRight) o));
		mapping.put(Country.class, o -> cleanCountry(vocabularies, (Country) o));
		mapping.put(Relation.class, o -> cleanRelation(vocabularies, (Relation) o));

		return mapping;
	}

	private static void cleanRelation(VocabularyGroup vocabularies, Relation r) {
		if (vocabularies.vocabularyExists(ModelConstants.DNET_RELATION_SUBRELTYPE)) {
			Qualifier newValue = vocabularies.lookup(ModelConstants.DNET_RELATION_SUBRELTYPE, r.getSubRelType());
			r.setSubRelType(newValue.getClassid());
		}
		if (vocabularies.vocabularyExists(ModelConstants.DNET_RELATION_RELCLASS)) {
			Qualifier newValue = vocabularies.lookup(ModelConstants.DNET_RELATION_RELCLASS, r.getRelClass());
			r.setRelClass(newValue.getClassid());
		}
	}

	private static void cleanCountry(VocabularyGroup vocabularies, Country o) {
		final Country c = o;
		if (StringUtils.isBlank(c.getSchemeid())) {
			c.setSchemeid(ModelConstants.DNET_COUNTRY_TYPE);
			c.setSchemename(ModelConstants.DNET_COUNTRY_TYPE);
		}
		cleanQualifier(vocabularies, c);
	}

	private static <Q extends Qualifier> void cleanQualifier(VocabularyGroup vocabularies, Q q) {
		if (vocabularies.vocabularyExists(q.getSchemeid())) {
			Qualifier newValue = vocabularies.lookup(q.getSchemeid(), q.getClassid());
			q.setClassid(newValue.getClassid());
			q.setClassname(newValue.getClassname());
		}
	}

}
