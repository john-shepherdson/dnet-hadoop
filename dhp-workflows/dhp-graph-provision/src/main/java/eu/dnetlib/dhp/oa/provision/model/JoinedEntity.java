package eu.dnetlib.dhp.oa.provision.model;

import java.io.Serializable;

public class JoinedEntity implements Serializable {

    private TypedRow entity;

    private Links links;

    public JoinedEntity() {
    }

    public TypedRow getEntity() {
        return entity;
    }

    public void setEntity(TypedRow entity) {
        this.entity = entity;
    }

    public Links getLinks() {
        return links;
    }

    public void setLinks(Links links) {
        this.links = links;
    }
}
