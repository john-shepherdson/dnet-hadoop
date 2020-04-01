package eu.dnetlib.dhp.oa.provision.model;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;
import eu.dnetlib.dhp.schema.oaf.Relation;

import java.io.Serializable;
import java.util.Map;

/**
 * Allows to sort relationships according to the priority defined in weights map.
 */
public class SortableRelationKey implements Comparable<SortableRelationKey>, Serializable {

    private String sourceId;
    private String targetId;

    private String relType;
    private String subRelType;
    private String relClass;

    private final static Map<String, Integer> weights = Maps.newHashMap();

    static {
        weights.put("outcome", 0);
        weights.put("supplement", 1);
        weights.put("publicationDataset", 2);
        weights.put("relationship", 3);
        weights.put("similarity", 4);
        weights.put("affiliation", 5);

        weights.put("provision", 6);
        weights.put("participation", 7);
        weights.put("dedup", 8);
    }

    public static SortableRelationKey from(final Relation r) {
        final SortableRelationKey s = new SortableRelationKey();
        s.setSourceId(r.getSource());
        s.setTargetId(r.getTarget());
        s.setRelType(r.getRelType());
        s.setSubRelType(r.getSubRelType());
        s.setRelClass(r.getRelClass());
        return s;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

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

    @Override
    public int compareTo(SortableRelationKey o) {
        return ComparisonChain.start()
                .compare(weights.get(getSubRelType()), weights.get(o.getSubRelType()))
                .compare(getSourceId(), o.getSourceId())
                .compare(getTargetId(), o.getTargetId())
                .result();
    }

}
