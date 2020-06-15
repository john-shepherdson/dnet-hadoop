
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

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
