
package eu.dnetlib.dhp.oa.provision.model;

import java.io.Serializable;

import com.google.common.base.Objects;

import eu.dnetlib.dhp.schema.oaf.Relation;

public class RelatedEntityWrapper implements Serializable {

	private static final long serialVersionUID = -2624854064081757234L;

	private Relation relation;
	private RelatedEntity target;

	public RelatedEntityWrapper() {
	}

	public RelatedEntityWrapper(Relation relation, RelatedEntity target) {
		this.relation = relation;
		this.target = target;
	}

	public Relation getRelation() {
		return relation;
	}

	public void setRelation(Relation relation) {
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
