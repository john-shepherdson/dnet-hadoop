package eu.dnetlib.dhp.graph;

import java.io.Serializable;
import java.util.List;

public class LinkedEntity implements Serializable {

    private TypedRow entity;

    private List<Tuple> links;

    public TypedRow getEntity() {
        return entity;
    }

    public LinkedEntity setEntity(TypedRow entity) {
        this.entity = entity;
        return this;
    }

    public List<Tuple> getLinks() {
        return links;
    }

    public LinkedEntity setLinks(List<Tuple> links) {
        this.links = links;
        return this;
    }
}
