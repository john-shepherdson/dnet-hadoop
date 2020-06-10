
package eu.dnetlib.dhp.oa.graph.clean;

import eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils;
import eu.dnetlib.dhp.oa.graph.raw.common.VocabularyGroup;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.MapFunction;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class NormalizeEmptyFields<T extends Oaf> implements MapFunction<T, T> {

	private VocabularyGroup vocabularies;

	public NormalizeEmptyFields(VocabularyGroup vocabularies) {
		this.vocabularies = vocabularies;
	}

	@Override
	public T call(T value) throws Exception {

		doNormalize(value);

		return value;
	}

	private void doNormalize(Object o) {
		if (Objects.isNull(o)) {
			return;
		}

		if (o instanceof Iterable) {
			for (Object oi : (Iterable) o) {
				doNormalize(oi);
			}
		} else {

			Class clazz = o.getClass();

			if (clazz.isPrimitive()
				|| o instanceof Integer
				|| o instanceof Double
				|| o instanceof Float
				|| o instanceof Long
				|| o instanceof Boolean
				|| o instanceof String) {
				return;
			} else {
				try {
					for (Field field : getAllFields(new LinkedList<>(), clazz)) {
						field.setAccessible(true);
						Object value = field.get(o);
						if (value instanceof Qualifier && Objects.isNull(value)) {
							field.set(o, OafMapperUtils.unknown("", ""));
						} else if (value instanceof Field && Objects.isNull(value)) {

						} else {
							doNormalize(value);
						}
					}
				} catch (IllegalAccessException | IllegalArgumentException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	private static List<Field> getAllFields(List<Field> fields, Class<?> clazz) {
		fields.addAll(Arrays.asList(clazz.getDeclaredFields()));

		final Class<?> superclass = clazz.getSuperclass();
		if (Objects.nonNull(superclass) && superclass.getPackage().equals(Oaf.class.getPackage())) {
			getAllFields(fields, superclass);
		}

		return fields;
	}

	public VocabularyGroup getVocabularies() {
		return vocabularies;
	}
}
