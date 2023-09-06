
package eu.dnetlib.pace;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import org.apache.commons.io.IOUtils;

import eu.dnetlib.pace.common.AbstractPaceFunctions;

public abstract class AbstractPaceTest extends AbstractPaceFunctions {

	protected String readFromClasspath(final String filename) {
		final StringWriter sw = new StringWriter();
		try {
			IOUtils.copy(getClass().getResourceAsStream(filename), sw);
			return sw.toString();
		} catch (final IOException e) {
			throw new RuntimeException("cannot load resource from classpath: " + filename);
		}
	}

	protected String title(final String s) {
		return s;
	}

	protected String person(final String s) {
		return s;
	}

	protected String url(final String s) {
		return s;
	}

	protected double[] array(final double[] a) {
		return a;
	}

	protected List<String> createFieldList(List<String> strings, String fieldName) {
		return strings;

	}
}
