package eu.dnetlib.dhp.oa.provision.model;

import eu.dnetlib.dhp.schema.oaf.Relation;

public class Tuple2 {

    private Relation relation;

    private RelatedEntity relatedEntity;

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
}
