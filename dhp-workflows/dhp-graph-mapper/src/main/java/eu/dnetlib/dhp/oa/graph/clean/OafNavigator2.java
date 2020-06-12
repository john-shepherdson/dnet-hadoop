
package eu.dnetlib.dhp.oa.graph.clean;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Oaf;

public class OafNavigator2 {

	public static <E extends Oaf> E apply(E oaf, Map<Class, Function<Object, Object>> mapping) {
		navigate(oaf, mapping);
		return oaf;
	}

	private static void navigate(Object o, Map<Class, Function<Object, Object>> mapping) {
		if (Objects.isNull(o) || isPrimitive(o)) {
			return;
		} else {
			try {
				for (Field field : getAllFields(o.getClass())) {
					//System.out.println("VISITING " + field.getName() + " in " + o.getClass());
					field.setAccessible(true);
					Object value = field.get(o);

					if (Objects.nonNull(value)) {
						final Class<?> fieldType = field.getType();
						if ((fieldType.isArray() && !fieldType.getComponentType().isPrimitive())) {
							Object[] fs = (Object[]) value;
							for (Object fi : fs) {
								navigate(fi, mapping);
							}
						}
						if (Iterable.class.isAssignableFrom(fieldType)) {
							Iterable fs = (Iterable) value;
							for (Object fi : fs) {
								navigate(fi, mapping);
							}
						} else {
							final Function<Object, Object> cleaningFn = mapping.get(value.getClass());
							if (Objects.nonNull(cleaningFn)) {
								final Object newValue = cleaningFn.apply(value);
								if (!Objects.equals(value, newValue)) {
									//System.out.println("PATCHING " + field.getName() + " " + value.getClass());
									//System.out.println("OLD VALUE " + getObjectMapper().writeValueAsString(value));
									//System.out.println("NEW VALUE " + getObjectMapper().writeValueAsString(newValue));
									field.set(o, newValue);
								}
							}
						}
					}
				}

			} catch (IllegalAccessException | IllegalArgumentException /*| JsonProcessingException*/ e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static ObjectMapper getObjectMapper() {
		final ObjectMapper mapper = new ObjectMapper();
		return mapper;
	}

	private static boolean isPrimitive(Object o) {
		return o.getClass().isPrimitive()
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
