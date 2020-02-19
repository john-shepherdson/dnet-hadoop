package eu.dnetlib.dhp.graph.model;

import eu.dnetlib.dhp.schema.oaf.Relation;

public class Tuple2 {

    private Relation relation;

    private RelatedEntity relatedEntity;

    public Relation getRelation() {
        return relation;
    }

    public Tuple2 setRelation(Relation relation) {
        this.relation = relation;
        return this;
    }

    public RelatedEntity getRelatedEntity() {
        return relatedEntity;
    }

    public Tuple2 setRelatedEntity(RelatedEntity relatedEntity) {
        this.relatedEntity = relatedEntity;
        return this;
    }
}
