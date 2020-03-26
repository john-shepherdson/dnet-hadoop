package eu.dnetlib.dhp.graph.model;

import java.io.Serializable;

public class TypedRow implements Serializable {

    private String sourceId;

    private String targetId;

    private Boolean deleted;

    private String type;

    private String relType;
    private String subRelType;
    private String relClass;

    private String oaf;

    public String getSourceId() {
        return sourceId;
    }

    public TypedRow setSourceId(String sourceId) {
        this.sourceId = sourceId;
        return this;
    }

    public String getTargetId() {
        return targetId;
    }

    public TypedRow setTargetId(String targetId) {
        this.targetId = targetId;
        return this;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public TypedRow setDeleted(Boolean deleted) {
        this.deleted = deleted;
        return this;
    }

    public String getType() {
        return type;
    }

    public TypedRow setType(String type) {
        this.type = type;
        return this;
    }

    public String getRelType() {
        return relType;
    }

    public TypedRow setRelType(String relType) {
        this.relType = relType;
        return this;
    }

    public String getSubRelType() {
        return subRelType;
    }

    public TypedRow setSubRelType(String subRelType) {
        this.subRelType = subRelType;
        return this;
    }

    public String getRelClass() {
        return relClass;
    }

    public TypedRow setRelClass(String relClass) {
        this.relClass = relClass;
        return this;
    }

    public String getOaf() {
        return oaf;
    }

    public TypedRow setOaf(String oaf) {
        this.oaf = oaf;
        return this;
    }
}
