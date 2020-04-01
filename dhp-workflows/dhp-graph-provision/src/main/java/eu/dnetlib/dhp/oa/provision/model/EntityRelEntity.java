package eu.dnetlib.dhp.oa.provision.model;

import eu.dnetlib.dhp.schema.oaf.Relation;

import java.io.Serializable;

public class EntityRelEntity implements Serializable {

    private TypedRow entity;
    private Relation relation;
    private RelatedEntity target;

    public TypedRow getEntity() {
        return entity;
    }

    public void setEntity(TypedRow entity) {
        this.entity = entity;
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
}
