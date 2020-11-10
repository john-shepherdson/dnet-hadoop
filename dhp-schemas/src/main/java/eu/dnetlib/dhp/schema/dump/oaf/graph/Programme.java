
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

/**
 * To store information about the ec programme for the project. It has the following parameters: - private String code
 * to store the code of the programme - private String description to store the description of the programme
 */
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
