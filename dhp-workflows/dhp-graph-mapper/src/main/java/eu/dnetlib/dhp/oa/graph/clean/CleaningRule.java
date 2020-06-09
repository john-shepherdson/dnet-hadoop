
package eu.dnetlib.dhp.oa.graph.clean;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import org.apache.spark.api.java.function.MapFunction;

import eu.dnetlib.dhp.oa.graph.raw.common.VocabularyGroup;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Qualifier;

public class CleaningRule<T extends Oaf> implements MapFunction<T, T> {

	private VocabularyGroup vocabularies;

	public CleaningRule(VocabularyGroup vocabularies) {
		this.vocabularies = vocabularies;
	}

	@Override
	public T call(T value) throws Exception {

		doClean(value);

		return value;
	}

	private void doClean(Object o) {
		if (Objects.isNull(o)) {
			return;
		}

		if (o instanceof Iterable) {
			for (Object oi : (Iterable) o) {
				doClean(oi);
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
						if (value instanceof Qualifier) {
							Qualifier q = (Qualifier) value;
							if (vocabularies.vocabularyExists(q.getSchemeid())) {

								field.set(o, vocabularies.lookup(q.getSchemeid(), q.getClassid()));
							}

						} else {
							doClean(value);
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
}
