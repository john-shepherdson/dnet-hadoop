
package eu.dnetlib.dhp.bulktag.community;

import eu.dnetlib.dhp.bulktag.resolver.RelatedEntity;

import java.io.Serializable;

public class TaggingConstraint extends SelectionConstraints implements Serializable {
	private String id;
	String entityToTag;
	String entityClass;
	private RelatedEntity relatedEntity;

	public RelatedEntity getRelatedEntity() {
		return relatedEntity;
	}

	public void setRelatedEntity(RelatedEntity relatedEntity) {
		this.relatedEntity = relatedEntity;
	}

	public String getEntityClass() {
		return entityClass;
	}

	public void setEntityClass(String entityClass) {
		this.entityClass = entityClass;
	}

	public String getEntityToTag() {
		return entityToTag;
	}

	public void setEntityToTag(String entityToTag) {
		this.entityToTag = entityToTag;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
}
