
package eu.dnetlib.dhp.bulktag.community;

import java.io.Serializable;

public class TaggingConstraint extends SelectionConstraints implements Serializable {
	private String id;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
}
