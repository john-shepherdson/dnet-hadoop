
package eu.dnetlib.dhp.oa.provision.model;

import java.io.Serializable;
import java.util.List;

public class JoinedEntity implements Serializable {

	private TypedRow entity;

	private List<Tuple2> links;

	public JoinedEntity() {
	}

	public TypedRow getEntity() {
		return entity;
	}

	public void setEntity(TypedRow entity) {
		this.entity = entity;
	}

	public List<Tuple2> getLinks() {
		return links;
	}

	public void setLinks(List<Tuple2> links) {
		this.links = links;
	}
}
