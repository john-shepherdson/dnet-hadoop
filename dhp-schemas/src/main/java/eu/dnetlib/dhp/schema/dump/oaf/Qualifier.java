
package eu.dnetlib.dhp.schema.dump.oaf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class Qualifier implements Serializable {

	private String code; //the classid in the Qualifier
	private String label; //the classname in the Qualifier

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

	public static Qualifier newInstance(String code, String value){
		Qualifier qualifier = new Qualifier();
		qualifier.setCode(code);
		qualifier.setLabel(value);
		return qualifier;
	}
}
