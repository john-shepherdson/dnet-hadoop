
package eu.dnetlib.dhp.countrypropagation;

import java.io.Serializable;

public class EntityEntityRel implements Serializable {
	private String entity1Id;
	private String entity2Id;

	public static EntityEntityRel newInstance(String source, String target) {
		EntityEntityRel dso = new EntityEntityRel();
		dso.entity1Id = source;
		dso.entity2Id = target;
		return dso;
	}

	public String getEntity1Id() {
		return entity1Id;
	}

	public void setEntity1Id(String entity1Id) {
		this.entity1Id = entity1Id;
	}

	public String getEntity2Id() {
		return entity2Id;
	}

	public void setEntity2Id(String entity2Id) {
		this.entity2Id = entity2Id;
	}
}
