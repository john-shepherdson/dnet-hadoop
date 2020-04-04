package eu.dnetlib.dhp.oa.provision.model;

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
}
