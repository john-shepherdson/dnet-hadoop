
package eu.dnetlib.dhp.oa.provision.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import eu.dnetlib.dhp.schema.oaf.OafEntity;

public class JoinedEntity<E extends OafEntity> implements Serializable {

	private E entity;

	private List<RelatedEntityWrapper> links;

	public JoinedEntity() {
		links = new LinkedList<>();
	}

	public JoinedEntity(E entity) {
		this();
		this.entity = entity;
	}

	public E getEntity() {
		return entity;
	}

	public void setEntity(E entity) {
		this.entity = entity;
	}

	public List<RelatedEntityWrapper> getLinks() {
		return links;
	}

	public void setLinks(List<RelatedEntityWrapper> links) {
		this.links = links;
	}
}
