package eu.dnetlib.dhp.schema.oaf;

import java.util.List;

public class Relation extends Oaf<Relation> {

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
        return self();
    }

    public String getSubRelType() {
        return subRelType;
    }

    public Relation setSubRelType(String subRelType) {
        this.subRelType = subRelType;
        return self();
    }

    public String getRelClass() {
        return relClass;
    }

    public Relation setRelClass(String relClass) {
        this.relClass = relClass;
        return self();
    }

    public String getSource() {
        return source;
    }

    public Relation setSource(String source) {
        this.source = source;
        return self();
    }

    public String getTarget() {
        return target;
    }

    public Relation setTarget(String target) {
        this.target = target;
        return self();
    }

    public List<KeyValue> getCollectedFrom() {
        return collectedFrom;
    }

    public Relation setCollectedFrom(List<KeyValue> collectedFrom) {
        this.collectedFrom = collectedFrom;
        return self();
    }

    @Override
    protected Relation self() {
        return this;
    }
}
