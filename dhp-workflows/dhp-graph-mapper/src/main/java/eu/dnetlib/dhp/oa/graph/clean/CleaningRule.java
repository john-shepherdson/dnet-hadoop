
package eu.dnetlib.dhp.oa.graph.clean;

import com.google.common.collect.Maps;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.MapFunction;

import eu.dnetlib.dhp.oa.graph.raw.common.VocabularyGroup;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Qualifier;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class CleaningRule<T extends Oaf> implements MapFunction<T, T> {

	private VocabularyGroup vocabularies;

	private Map<Class, Function<Object, Object>> mapping = Maps.newHashMap();


	public CleaningRule(VocabularyGroup vocabularies) {
		this.vocabularies = vocabularies;

		mapping.put(Qualifier.class, o -> patchQualifier(o));
		mapping.put(StructuredProperty.class, o -> patchSp(o));
		mapping.put(Field.class, o -> patchStringField(o));
	}

	@Override
	public T call(T value) throws Exception {

		OafNavigator.apply(value, mapping);

		return value;
	}

	private Object patchQualifier(Object o) {
		Qualifier q = (Qualifier) o;
		if (vocabularies.vocabularyExists(q.getSchemeid())) {
			return vocabularies.lookup(q.getSchemeid(), q.getClassid());
		}
		return o;
	}

	private Object patchSp(Object o) {
		StructuredProperty sp = (StructuredProperty) o;
		if (StringUtils.isBlank(sp.getValue())) {
			return null;
		}
		return o;
	}

	private Object patchStringField(Object o) {
		Field f = (Field) o;
		try {
			if (StringUtils.isBlank((String) f.getValue())) {
				return null;
			}
		} catch (ClassCastException e) {
			// ignored on purpose
		}

		return o;
	}

	public VocabularyGroup getVocabularies() {
		return vocabularies;
	}
}
