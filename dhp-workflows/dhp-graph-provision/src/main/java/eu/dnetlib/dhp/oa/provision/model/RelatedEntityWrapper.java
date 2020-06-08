
package eu.dnetlib.dhp.oa.provision.model;

import java.io.Serializable;

import com.google.common.base.Objects;

public class RelatedEntityWrapper implements Serializable {

	private SortableRelation relation;
	private RelatedEntity target;

	public RelatedEntityWrapper() {
	}

	public RelatedEntityWrapper(SortableRelation relation, RelatedEntity target) {
		this(null, relation, target);
	}

	public RelatedEntityWrapper(TypedRow entity, SortableRelation relation, RelatedEntity target) {
		this.relation = relation;
		this.target = target;
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
		RelatedEntityWrapper that = (RelatedEntityWrapper) o;
		return Objects.equal(relation, that.relation)
			&& Objects.equal(target, that.target);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(relation, target);
	}
}
