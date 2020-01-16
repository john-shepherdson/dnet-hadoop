package eu.dnetlib.dhp.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.Oaf;

import java.io.Serializable;

public class TypedRow implements Serializable {
    private String type;
    private Oaf oaf;

    public TypedRow(String type, Oaf oaf) {
        this.type = type;
        this.oaf = oaf;
    }

    public String getType() {
        return type;
    }

    public TypedRow setType(String type) {
        this.type = type;
        return this;
    }

    public Oaf getOaf() {
        return oaf;
    }

    public TypedRow setOaf(Oaf oaf) {
        this.oaf = oaf;
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
