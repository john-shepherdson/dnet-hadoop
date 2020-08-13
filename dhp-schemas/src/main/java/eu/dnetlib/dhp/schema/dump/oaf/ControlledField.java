
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

/**
 * To represent the information described by a scheme and a value in that scheme (i.e. pid).
 * It has two parameters:
 *       - scheme of type String to store the scheme
 *       - value of type String to store the value in that scheme
 */
public class ControlledField implements Serializable {
	private String scheme;
	private String value;

	public String getScheme() {
		return scheme;
	}

	public void setScheme(String scheme) {
		this.scheme = scheme;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public static ControlledField newInstance(String scheme, String value) {
		ControlledField cf = new ControlledField();

		cf.setScheme(scheme);
		cf.setValue(value);

		return cf;
	}
}
