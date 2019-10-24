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

    public void setRelType(String relType) {
        this.relType = relType;
    }

    public String getSubRelType() {
        return subRelType;
    }

    public void setSubRelType(String subRelType) {
        this.subRelType = subRelType;
    }

    public String getRelClass() {
        return relClass;
    }

    public void setRelClass(String relClass) {
        this.relClass = relClass;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public List<KeyValue> getCollectedFrom() {
        return collectedFrom;
    }

    public void setCollectedFrom(List<KeyValue> collectedFrom) {
        this.collectedFrom = collectedFrom;
    }
}
