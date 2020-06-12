
package eu.dnetlib.dhp.oa.graph.clean;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Consumer;

import eu.dnetlib.dhp.schema.oaf.Oaf;

public class OafNavigator {

	public static <E extends Oaf> E apply(E oaf, Map<Class, Consumer<Object>> mapping) {
		try {
			navigate(oaf, mapping);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		return oaf;
	}

	private static void navigate(Object o, Map<Class, Consumer<Object>> mapping) throws IllegalAccessException {
		if (isPrimitive(o)) {
			return;
		} else if (isIterable(o.getClass())) {
			for (final Object elem : (Iterable<?>) o) {
				navigate(elem, mapping);
			}
		} else if (hasMapping(o, mapping)) {
			mapping.get(o.getClass()).accept(o);
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

	private static boolean hasMapping(Object o, Map<Class, Consumer<Object>> mapping) {
		return mapping.containsKey(o.getClass());
	}

	private static boolean isIterable(final Class<?> cl) {
		return Iterable.class.isAssignableFrom(cl);
	}

	private static boolean isPrimitive(Object o) {
		return Objects.isNull(o)
			|| o.getClass().isPrimitive()
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
