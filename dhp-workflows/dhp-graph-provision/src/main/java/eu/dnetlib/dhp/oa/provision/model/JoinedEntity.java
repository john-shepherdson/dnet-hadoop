
package eu.dnetlib.dhp.oa.provision.model;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Lists;

import eu.dnetlib.dhp.schema.common.EntityType;
import eu.dnetlib.dhp.schema.oaf.*;

public class JoinedEntity<T> implements Serializable {

	private static final long serialVersionUID = -6337458773099581114L;

	private T entity;

	private List<RelatedEntityWrapper> links;

	public JoinedEntity() {
		this(null, new LinkedList<>());
	}

	public JoinedEntity(T entity) {
		this(entity, Lists.newLinkedList());
	}

	public JoinedEntity(T entity, List<RelatedEntityWrapper> links) {
		this.entity = entity;
		this.links = links;
	}

	public T getEntity() {
		return entity;
	}

	public void setEntity(T entity) {
		this.entity = entity;
	}

	public List<RelatedEntityWrapper> getLinks() {
		return links;
	}

	public void setLinks(List<RelatedEntityWrapper> links) {
		this.links = links;
	}
}
