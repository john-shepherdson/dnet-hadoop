
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;

public class Relationship implements Serializable {

	private String type;
	private String id;
	private String label;
	private final static long serialVersionUID = 7847399503395576960L;

	public String getType() {
		return type;
	}

	public void setType(final String type) {
		this.type = type;
	}

	public String getId() {
		return id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(final String label) {
		this.label = label;
	}

}
