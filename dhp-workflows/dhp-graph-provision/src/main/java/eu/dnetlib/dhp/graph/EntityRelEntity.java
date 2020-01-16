package eu.dnetlib.dhp.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.Relation;

import java.io.Serializable;

public class EntityRelEntity implements Serializable {
    private TypedRow source;
    private Relation relation;
    private TypedRow target;

    public EntityRelEntity(TypedRow source) {
        this.source = source;
    }

    public TypedRow getSource() {
        return source;
    }

    public EntityRelEntity setSource(TypedRow source) {
        this.source = source;
        return this;
    }

    public Relation getRelation() {
        return relation;
    }

    public EntityRelEntity setRelation(Relation relation) {
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

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
