package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.schema.oaf.OafEntity;

import java.io.Serializable;
import java.util.List;

public class LinkedEntity implements Serializable {

    private String type;

    private OafEntity entity;

    private List<Link> links;

    public String getType() {
        return type;
    }

    public LinkedEntity setType(String type) {
        this.type = type;
        return this;
    }

    public OafEntity getEntity() {
        return entity;
    }

    public LinkedEntity setEntity(OafEntity entity) {
        this.entity = entity;
        return this;
    }

    public List<Link> getLinks() {
        return links;
    }

    public LinkedEntity setLinks(List<Link> links) {
        this.links = links;
        return this;
    }
}
