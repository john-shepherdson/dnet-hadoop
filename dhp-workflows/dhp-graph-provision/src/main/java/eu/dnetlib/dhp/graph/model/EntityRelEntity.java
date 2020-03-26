package eu.dnetlib.dhp.graph.model;

import java.io.Serializable;

public class EntityRelEntity implements Serializable {

    private TypedRow source;
    private TypedRow relation;
    private TypedRow target;

    public EntityRelEntity() {
    }

    public EntityRelEntity(TypedRow source) {
        this.source = source;
    }

    //helpers
    public Boolean hasMainEntity() {
        return getSource() != null & getRelation() == null & getTarget() == null;
    }

    public Boolean hasRelatedEntity() {
        return getSource() == null & getRelation() != null & getTarget() != null;
    }


    public TypedRow getSource() {
        return source;
    }

    public EntityRelEntity setSource(TypedRow source) {
        this.source = source;
        return this;
    }

    public TypedRow getRelation() {
        return relation;
    }

    public EntityRelEntity setRelation(TypedRow relation) {
        this.relation = relation;
        return this;
    }

    public TypedRow getTarget() {
        return target;
    }

    public EntityRelEntity setTarget(TypedRow target) {
        this.target = target;
        return this;
    }
}
