package eu.dnetlib.dhp.graph;

import java.io.Serializable;

public class TypedRow implements Serializable {

    private String source;
    private String target;
    private String type;
    private String oaf;

    public TypedRow() {
    }

    public TypedRow(String source, String type, String oaf) {
        this.source = source;
        this.type = type;
        this.oaf = oaf;
    }

    public TypedRow(String source, String target, String type, String oaf) {
        this(source, type, oaf);
        this.target = target;
    }

    public String getSource() {
        return source;
    }

    public TypedRow setSource(String source) {
        this.source = source;
        return this;
    }

    public String getTarget() {
        return target;
    }

    public TypedRow setTarget(String target) {
        this.target = target;
        return this;
    }

    public String getType() {
        return type;
    }

    public TypedRow setType(String type) {
        this.type = type;
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
