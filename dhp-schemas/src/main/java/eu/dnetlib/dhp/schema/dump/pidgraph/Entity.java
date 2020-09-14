
package eu.dnetlib.dhp.schema.dump.pidgraph;

import java.io.Serializable;

public class Entity implements Serializable {
	private String id;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public static Entity newInstance(String id) {
		Entity entity = new Entity();
		entity.id = id;

		return entity;
	}
}
