
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

public class Fundings implements Serializable {

	private String id;
	private String description;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}
