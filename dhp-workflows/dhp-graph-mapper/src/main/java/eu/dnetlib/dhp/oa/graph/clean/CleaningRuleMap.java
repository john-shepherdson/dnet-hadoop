
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

public class CleaningRuleMap extends HashMap<Class, SerializableConsumer<Object>> implements Serializable {

	/**
	 * Creates the mapping for the Oaf types subject to cleaning
	 *
	 * @param vocabularies
	 */
	public static CleaningRuleMap create(VocabularyGroup vocabularies) {
		CleaningRuleMap mapping = new CleaningRuleMap();
		mapping.put(Qualifier.class, o -> cleanQualifier(vocabularies, (Qualifier) o));
		mapping.put(AccessRight.class, o -> cleanQualifier(vocabularies, (AccessRight) o));
		mapping.put(Country.class, o -> {
			final Country c = (Country) o;
			if (StringUtils.isBlank(c.getSchemeid())) {
				c.setSchemeid(ModelConstants.DNET_COUNTRY_TYPE);
				c.setSchemename(ModelConstants.DNET_COUNTRY_TYPE);
			}
			cleanQualifier(vocabularies, c);
		});
		return mapping;
	}

	private static <Q extends Qualifier> void cleanQualifier(VocabularyGroup vocabularies, Q q) {
		if (vocabularies.vocabularyExists(q.getSchemeid())) {
			Qualifier newValue = vocabularies.lookup(q.getSchemeid(), q.getClassid());
			q.setClassid(newValue.getClassid());
			q.setClassname(newValue.getClassname());
		}
	}

}
