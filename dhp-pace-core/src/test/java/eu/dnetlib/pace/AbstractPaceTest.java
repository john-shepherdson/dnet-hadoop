
package eu.dnetlib.pace;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;

import eu.dnetlib.pace.common.AbstractPaceFunctions;
import eu.dnetlib.pace.config.Type;
import eu.dnetlib.pace.model.Field;
import eu.dnetlib.pace.model.FieldListImpl;
import eu.dnetlib.pace.model.FieldValueImpl;

public abstract class AbstractPaceTest extends AbstractPaceFunctions {

	protected String readFromClasspath(final String filename) {
		final StringWriter sw = new StringWriter();
		try {
			IOUtils.copy(getClass().getResourceAsStream(filename), sw, StandardCharsets.UTF_8);
			return sw.toString();
		} catch (final IOException e) {
			throw new RuntimeException("cannot load resource from classpath: " + filename);
		}
	}

	protected Field title(final String s) {
		return new FieldValueImpl(Type.String, "title", s);
	}

	protected Field person(final String s) {
		return new FieldValueImpl(Type.JSON, "person", s);
	}

	protected Field url(final String s) {
		return new FieldValueImpl(Type.URL, "url", s);
	}

	protected Field array(final double[] a) {
		return new FieldValueImpl(Type.DoubleArray, "array", a);
	}

	protected Field createFieldList(List<String> strings, String fieldName) {

		List<FieldValueImpl> fieldValueStream = strings
			.stream()
			.map(s -> new FieldValueImpl(Type.String, fieldName, s))
			.collect(Collectors.toList());

		FieldListImpl a = new FieldListImpl();
		a.addAll(fieldValueStream);

		return a;

	}
}
