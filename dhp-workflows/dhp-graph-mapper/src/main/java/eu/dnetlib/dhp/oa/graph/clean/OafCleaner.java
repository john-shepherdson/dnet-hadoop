
package eu.dnetlib.dhp.oa.graph.clean;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import eu.dnetlib.dhp.schema.oaf.Oaf;

public class OafCleaner implements Serializable {

	public static <E extends Oaf> E apply(E oaf, CleaningRuleMap mapping) {
		try {
			navigate(oaf, mapping);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException(e);
		}
		return oaf;
	}

	private static void navigate(Object o, CleaningRuleMap mapping) throws IllegalAccessException {
		if (isPrimitive(o)) {
			return;
		} else if (isIterable(o.getClass())) {
			for (final Object elem : (Iterable<?>) o) {
				navigate(elem, mapping);
			}
		} else if (hasMapping(o, mapping)) {
			mapping.get(o.getClass()).accept(o);
			for (final Field f : getAllFields(o.getClass())) {
				f.setAccessible(true);
				final Object val = f.get(o);
				navigate(val, mapping);
			}
		} else {
			for (final Field f : getAllFields(o.getClass())) {
				f.setAccessible(true);
				final Object val = f.get(o);
				if (!isPrimitive(val) && hasMapping(val, mapping)) {
					mapping.get(val.getClass()).accept(val);
				} else {
					navigate(f.get(o), mapping);
				}
			}
		}
	}

	private static boolean hasMapping(Object o, CleaningRuleMap mapping) {
		return mapping.containsKey(o.getClass());
	}

	private static boolean isIterable(final Class<?> cl) {
		return Iterable.class.isAssignableFrom(cl);
	}

	private static boolean isPrimitive(Object o) {
		return Objects.isNull(o)
			|| o.getClass().isPrimitive()
			|| o.getClass().isEnum()
			|| o instanceof Class
			|| o instanceof Integer
			|| o instanceof Double
			|| o instanceof Float
			|| o instanceof Long
			|| o instanceof Boolean
			|| o instanceof String
			|| o instanceof Byte;
	}

	private static List<Field> getAllFields(Class<?> clazz) {
		return getAllFields(new LinkedList<>(), clazz);
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
