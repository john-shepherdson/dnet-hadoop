package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Relation;

import java.io.Serializable;

public class Tuple implements Serializable {

    private TypedRow relation;

    private TypedRow target;

    public TypedRow getRelation() {
        return relation;
    }

    public Tuple setRelation(TypedRow relation) {
        this.relation = relation;
        return this;
    }

    public TypedRow getTarget() {
        return target;
    }

    public Tuple setTarget(TypedRow target) {
        this.target = target;
        return this;
    }
}
