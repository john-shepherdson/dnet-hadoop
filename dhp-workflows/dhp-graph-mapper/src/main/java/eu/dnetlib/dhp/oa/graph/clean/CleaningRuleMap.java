
package eu.dnetlib.dhp.oa.graph.clean;

import java.io.Serializable;
import java.util.HashMap;

import eu.dnetlib.dhp.common.FunctionalInterfaceSupport.SerializableConsumer;
import eu.dnetlib.dhp.oa.graph.raw.common.VocabularyGroup;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class CleaningRuleMap extends HashMap<Class, SerializableConsumer<Object>> implements Serializable {

	/**
	 * Creates the mapping for the Oaf types subject to cleaning
	 *
	 * @param vocabularies
	 */
	public static CleaningRuleMap create(VocabularyGroup vocabularies) {
		CleaningRuleMap mapping = new CleaningRuleMap();
		mapping.put(Qualifier.class, o -> {
			Qualifier q = (Qualifier) o;
			if (vocabularies.vocabularyExists(q.getSchemeid())) {
				Qualifier newValue = vocabularies.lookup(q.getSchemeid(), q.getClassid());
				q.setClassid(newValue.getClassid());
				q.setClassname(newValue.getClassname());
			}
		});
		mapping.put(StructuredProperty.class, o -> {
			StructuredProperty sp = (StructuredProperty) o;
			// TODO implement a policy
			/*
			 * if (StringUtils.isBlank(sp.getValue())) { sp.setValue(null); sp.setQualifier(null); sp.setDataInfo(null);
			 * }
			 */
		});
		return mapping;
	}

}
