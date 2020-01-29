package eu.dnetlib.dhp.graph;

import java.io.Serializable;

public class TupleWrapper implements Serializable {

    private TypedRow relation;

    private TypedRow target;


    public TypedRow getRelation() {
        return relation;
    }

    public TupleWrapper setRelation(TypedRow relation) {
        this.relation = relation;
        return this;
    }

    public TypedRow getTarget() {
        return target;
    }

    public TupleWrapper setTarget(TypedRow target) {
        this.target = target;
        return this;
    }
}
