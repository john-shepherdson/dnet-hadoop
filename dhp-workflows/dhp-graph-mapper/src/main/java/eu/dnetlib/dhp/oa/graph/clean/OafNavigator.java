
package eu.dnetlib.dhp.oa.graph.clean;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import scala.Tuple2;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.function.Function;

public class OafNavigator {

	public static <E extends Oaf> E apply(E oaf, Map<Class, Function<Object, Object>> mapping) {
		reflect(oaf, mapping);
		return oaf;
	}

	public static void reflect(Object o, Map<Class, Function<Object, Object>> mapping) {
		visit(o, mapping);
	}

	public static void visit(final Object thingy, Map<Class, Function<Object, Object>> mapping) {

		try {
			final Class<?> clazz = thingy.getClass();

			if (!isPrimitive(thingy) && clazz.getPackage().equals(Oaf.class.getPackage())) {

				final BeanInfo beanInfo = Introspector.getBeanInfo(clazz);

				for (final PropertyDescriptor descriptor : beanInfo.getPropertyDescriptors()) {
					try {
						final Object value = descriptor.getReadMethod().invoke(thingy);

						if (value != null && !isPrimitive(value)) {

							System.out.println("VISITING " + descriptor.getName() + " " + descriptor.getPropertyType());

							if (Iterable.class.isAssignableFrom(descriptor.getPropertyType())) {
								for(Object vi : (Iterable) value) {

									visit(vi, mapping);
								}
							} else {

								if (mapping.keySet().contains(value.getClass())) {
									final Object newValue = mapping.get(value.getClass()).apply(value);
									System.out.println("PATCHING " + descriptor.getName()+ " " + descriptor.getPropertyType());
									System.out.println("OLD VALUE " + getObjectMapper().writeValueAsString(value));
									System.out.println("NEW VALUE " + getObjectMapper().writeValueAsString(newValue));
									descriptor.getWriteMethod().invoke(newValue);
								}

								visit(value, mapping);
							}
						}

					} catch (final IllegalArgumentException e) {
						// handle this please
					} catch (final IllegalAccessException e) {
						// and this also
					} catch (final InvocationTargetException e) {
						// and this, too
					} catch (JsonProcessingException e) {
						e.printStackTrace();
					}
				}
			}
		} catch (final IntrospectionException e) {
			// do something sensible here
		}
	}

	private static ObjectMapper getObjectMapper() {
		final ObjectMapper mapper = new ObjectMapper();
		return mapper;
	}

	private static void navigate(Object o, Map<Class, Function<Object, Object>> mapping) {
		if (Objects.isNull(o) || isPrimitive(o)) {
			return;
		} else {
			try {
				for (Field field : getAllFields(o.getClass())) {
					System.out.println(field.getName());
					field.setAccessible(true);
					Object value = field.get(o);

					if (Objects.nonNull(value)) {
						final Class<?> fieldType = field.getType();
						if ((fieldType.isArray() && !fieldType.getComponentType().isPrimitive())) {
							Object[] fs = (Object[]) value;
							for (Object fi : fs) {
								navigate(fi, mapping);
							}
						} if (Iterable.class.isAssignableFrom(fieldType)) {
							Iterable fs = (Iterable) value;
							for (Object fi : fs) {
								navigate(fi, mapping);
							}
						} else {
							if (mapping.keySet().contains(value.getClass())) {
								System.out.println("PATCHING " + field.getName());
								field.set(o, mapping.get(value.getClass()).apply(value));
							}
						}
					}
				}

			} catch (IllegalAccessException | IllegalArgumentException e) {
				throw new RuntimeException(e);
			}
		}
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
