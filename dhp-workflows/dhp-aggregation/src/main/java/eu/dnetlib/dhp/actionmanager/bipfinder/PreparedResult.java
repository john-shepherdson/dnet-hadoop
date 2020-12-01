
package eu.dnetlib.dhp.actionmanager.bipfinder;

import java.io.Serializable;

/**
 * Subset of the information of the generic results that are needed to create the atomic action
 */
public class PreparedResult implements Serializable {
	private String id; // openaire id
	private String value; // doi

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
