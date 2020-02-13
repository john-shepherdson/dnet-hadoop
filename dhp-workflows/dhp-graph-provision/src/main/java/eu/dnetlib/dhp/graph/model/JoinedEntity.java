package eu.dnetlib.dhp.graph.model;

import eu.dnetlib.dhp.schema.oaf.OafEntity;

import java.io.Serializable;

public class JoinedEntity implements Serializable {

    private String type;

    private OafEntity entity;

    private Links links;

    public String getType() {
        return type;
    }

    public JoinedEntity setType(String type) {
        this.type = type;
        return this;
    }

    public OafEntity getEntity() {
        return entity;
    }

    public JoinedEntity setEntity(OafEntity entity) {
        this.entity = entity;
        return this;
    }

    public Links getLinks() {
        return links;
    }

    public JoinedEntity setLinks(Links links) {
        this.links = links;
        return this;
    }
}
