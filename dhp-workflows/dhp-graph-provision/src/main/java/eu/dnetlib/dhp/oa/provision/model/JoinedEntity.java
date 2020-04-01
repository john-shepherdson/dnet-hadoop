package eu.dnetlib.dhp.oa.provision.model;

import eu.dnetlib.dhp.schema.oaf.OafEntity;

import java.io.Serializable;

public class JoinedEntity implements Serializable {

    private String type;

    private OafEntity entity;

    private Links links;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public OafEntity getEntity() {
        return entity;
    }

    public void setEntity(OafEntity entity) {
        this.entity = entity;
    }

    public Links getLinks() {
        return links;
    }

    public void setLinks(Links links) {
        this.links = links;
    }
}
