
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * To represent the information described by a code and a value It has two parameters:
 * - code to store the code (generally the classid of the eu.dnetlib.dhp.schema.oaf.Qualifier element)
 * - label to store the label (generally the classname of the eu.dnetlib.dhp.schema.oaf.Qualifier element
 */
public class Qualifier implements Serializable {

	private String code; // the classid in the Qualifier
	private String label; // the classname in the Qualifier

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public static Qualifier newInstance(String code, String value) {
		Qualifier qualifier = new Qualifier();
		qualifier.setCode(code);
		qualifier.setLabel(value);
		return qualifier;
	}
}
