package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.schema.oaf.Relation;

import java.io.Serializable;

public class Link implements Serializable {

    private Relation relation;

    private RelatedEntity relatedEntity;

    public Relation getRelation() {
        return relation;
    }

    public Link setRelation(Relation relation) {
        this.relation = relation;
        return this;
    }

    public RelatedEntity getRelatedEntity() {
        return relatedEntity;
    }

    public Link setRelatedEntity(RelatedEntity relatedEntity) {
        this.relatedEntity = relatedEntity;
        return this;
    }
}
