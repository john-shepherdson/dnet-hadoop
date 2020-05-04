
package eu.dnetlib.dhp.oa.provision.model;

import java.io.Serializable;
import java.util.Objects;

import eu.dnetlib.dhp.schema.oaf.Relation;

public class Tuple2 implements Serializable {

	private Relation relation;

	private RelatedEntity relatedEntity;

	public Tuple2() {
	}

	public Tuple2(Relation relation, RelatedEntity relatedEntity) {
		this.relation = relation;
		this.relatedEntity = relatedEntity;
	}

	public Relation getRelation() {
		return relation;
	}

	public void setRelation(Relation relation) {
		this.relation = relation;
	}

	public RelatedEntity getRelatedEntity() {
		return relatedEntity;
	}

	public void setRelatedEntity(RelatedEntity relatedEntity) {
		this.relatedEntity = relatedEntity;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Tuple2 t2 = (Tuple2) o;
		return getRelation().equals(t2.getRelation());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getRelation().hashCode());
	}
}
