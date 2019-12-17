package eu.dnetlib.dhp.graph;

import java.io.Serializable;

public class RelatedEntity implements Serializable {

    private String relType;

    private String subRelType;

    private String relClass;

    private String type;

    private String payload;

    public RelatedEntity(String relType, String subRelType, String relClass, String type, String payload) {
        this.relType = relType;
        this.subRelType = subRelType;
        this.relClass = relClass;
        this.type = type;
        this.payload = payload;
    }

    public String getRelType() {
        return relType;
    }

    public RelatedEntity setRelType(String relType) {
        this.relType = relType;
        return this;
    }

    public String getSubRelType() {
        return subRelType;
    }

    public RelatedEntity setSubRelType(String subRelType) {
        this.subRelType = subRelType;
        return this;
    }

    public String getRelClass() {
        return relClass;
    }

    public RelatedEntity setRelClass(String relClass) {
        this.relClass = relClass;
        return this;
    }

    public String getType() {
        return type;
    }

    public RelatedEntity setType(String type) {
        this.type = type;
        return this;
    }

    public String getPayload() {
        return payload;
    }

    public RelatedEntity setPayload(String payload) {
        this.payload = payload;
        return this;
    }
}
