
package eu.dnetlib.dhp.oa.provision.model;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.oaf.*;

public class JoinedEntity implements Serializable {

	private OafEntity entity;

	private List<RelatedEntityWrapper> links;

	public JoinedEntity() {
		links = new LinkedList<>();
	}

	public JoinedEntity(OafEntity entity) {
		this();
		this.entity = entity;
	}

	public OafEntity getEntity() {
		return entity;
	}

	public void setEntity(OafEntity entity) {
		this.entity = entity;
	}

	public List<RelatedEntityWrapper> getLinks() {
		return links;
	}

	public void setLinks(List<RelatedEntityWrapper> links) {
		this.links = links;
	}
}
