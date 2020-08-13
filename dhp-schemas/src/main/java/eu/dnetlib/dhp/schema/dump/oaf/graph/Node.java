
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

/**
 * To represent the generic node in a relation. It has the following parameters:
 * - private String id the openaire id of the entity in the relation
 * - private String type the type of the entity in the relation.
 *
 * Consider the generic relation between a Result R and a Project P, the node representing R will have
 * as id the id of R and as type result, while the node representing the project will have as id the id of the project
 * and as type project
 */
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
