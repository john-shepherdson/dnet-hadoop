
package eu.dnetlib.dhp.common.api.zenodo;

import java.io.Serializable;

public class Grant implements Serializable {
	private String id;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public static Grant newInstance(String id) {
		Grant g = new Grant();
		g.id = id;

		return g;
	}
}
