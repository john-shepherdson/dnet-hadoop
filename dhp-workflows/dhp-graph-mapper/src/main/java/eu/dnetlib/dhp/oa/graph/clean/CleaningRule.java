
package eu.dnetlib.dhp.oa.graph.clean;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.spark.api.java.function.MapFunction;

import com.google.common.collect.Maps;

import eu.dnetlib.dhp.oa.graph.raw.common.VocabularyGroup;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class CleaningRule<T extends Oaf> implements MapFunction<T, T> {

	private VocabularyGroup vocabularies;

	private Map<Class, Consumer<Object>> mapping = Maps.newHashMap();

	public CleaningRule(VocabularyGroup vocabularies) {
		this.vocabularies = vocabularies;
		setMappings(vocabularies);
	}

	@Override
	public T call(T value) throws Exception {

		OafNavigator.apply(value, mapping);

		return value;
	}

	/**
	 * Populates the mapping for the Oaf types subject to cleaning
	 * 
	 * @param vocabularies
	 */
	private void setMappings(VocabularyGroup vocabularies) {
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
	}

	public VocabularyGroup getVocabularies() {
		return vocabularies;
	}

}
