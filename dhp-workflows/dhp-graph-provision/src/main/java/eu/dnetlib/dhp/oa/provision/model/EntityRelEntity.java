
package eu.dnetlib.dhp.oa.provision.model;

import com.google.common.base.Objects;
import java.io.Serializable;

public class EntityRelEntity implements Serializable {

	private TypedRow entity;
	private SortableRelation relation;
	private RelatedEntity target;

	public EntityRelEntity() {
	}

	public EntityRelEntity(SortableRelation relation, RelatedEntity target) {
		this(null, relation, target);
	}

	public EntityRelEntity(TypedRow entity, SortableRelation relation, RelatedEntity target) {
		this.entity = entity;
		this.relation = relation;
		this.target = target;
	}

	public TypedRow getEntity() {
		return entity;
	}

	public void setEntity(TypedRow entity) {
		this.entity = entity;
	}

	public SortableRelation getRelation() {
		return relation;
	}

	public void setRelation(SortableRelation relation) {
		this.relation = relation;
	}

	public RelatedEntity getTarget() {
		return target;
	}

	public void setTarget(RelatedEntity target) {
		this.target = target;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		EntityRelEntity that = (EntityRelEntity) o;
		return Objects.equal(entity, that.entity)
			&& Objects.equal(relation, that.relation)
			&& Objects.equal(target, that.target);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(entity, relation, target);
	}
}
