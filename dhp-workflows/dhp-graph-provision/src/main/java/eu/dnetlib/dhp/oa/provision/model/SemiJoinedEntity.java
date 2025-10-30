package eu.dnetlib.dhp.oa.provision.model;

import java.io.Serializable;
import java.util.List;

public class SemiJoinedEntity implements Serializable {

    private String id;

    private List<RelatedEntityWrapper> links;

    public SemiJoinedEntity() {
    }

    public SemiJoinedEntity(String id, List<RelatedEntityWrapper> links) {
        this.id = id;
        this.links = links;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<RelatedEntityWrapper> getLinks() {
        return links;
    }

    public void setLinks(List<RelatedEntityWrapper> links) {
        this.links = links;
    }
}
