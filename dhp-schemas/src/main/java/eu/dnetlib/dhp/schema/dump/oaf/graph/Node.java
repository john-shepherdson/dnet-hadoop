
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

public class Node implements Serializable {
	private String id;
	private String type;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public static Node newInstance(String id, String type) {
		Node node = new Node();
		node.id = id;
		node.type = type;
		return node;
	}
}
