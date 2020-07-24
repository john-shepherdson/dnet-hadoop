
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

public class Programme implements Serializable {
	private String code;
	private String description;

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public static Programme newInstance(String code, String description) {
		Programme p = new Programme();
		p.code = code;
		p.description = description;
		return p;
	}
}
