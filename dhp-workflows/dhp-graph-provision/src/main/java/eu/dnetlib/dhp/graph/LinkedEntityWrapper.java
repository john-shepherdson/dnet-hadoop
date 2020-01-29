package eu.dnetlib.dhp.graph;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class LinkedEntityWrapper implements Serializable {

    private TypedRow entity;

    private List<TupleWrapper> links;

    public static LinkedEntityWrapper parse(final String s) {
        try {
            return new ObjectMapper().readValue(s, LinkedEntityWrapper.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("unable to decode LinkedEntityWrapper: " + s);
        }
    }

    public TypedRow getEntity() {
        return entity;
    }

    public LinkedEntityWrapper setEntity(TypedRow entity) {
        this.entity = entity;
        return this;
    }

    public List<TupleWrapper> getLinks() {
        return links;
    }

    public LinkedEntityWrapper setLinks(List<TupleWrapper> links) {
        this.links = links;
        return this;
    }
}
