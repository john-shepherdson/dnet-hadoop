package eu.dnetlib.dhp.schema.oaf;

import java.util.List;

public class Relation extends Oaf {

    private String relType;

    private String subRelType;

    private String relClass;

    private String source;

    private String target;

    private List<KeyValue> collectedFrom;

    public String getRelType() {
        return relType;
    }

    public Relation setRelType(String relType) {
        this.relType = relType;
        return this;
    }

    public String getSubRelType() {
        return subRelType;
    }

    public Relation setSubRelType(String subRelType) {
        this.subRelType = subRelType;
        return this;
    }

    public String getRelClass() {
        return relClass;
    }

    public Relation setRelClass(String relClass) {
        this.relClass = relClass;
        return this;
    }

    public String getSource() {
        return source;
    }

    public Relation setSource(String source) {
        this.source = source;
        return this;
    }

    public String getTarget() {
        return target;
    }

    public Relation setTarget(String target) {
        this.target = target;
        return this;
    }

    public List<KeyValue> getCollectedFrom() {
        return collectedFrom;
    }

    public Relation setCollectedFrom(List<KeyValue> collectedFrom) {
        this.collectedFrom = collectedFrom;
        return this;
    }
}
