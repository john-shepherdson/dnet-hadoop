package eu.dnetlib.dhp.graph.model;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;

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

    public static SortableRelationKey from(final EntityRelEntity e) {
        return new SortableRelationKey()
                .setSourceId(e.getRelation().getSourceId())
                .setTargetId(e.getRelation().getTargetId())
                .setRelType(e.getRelation().getRelType())
                .setSubRelType(e.getRelation().getSubRelType())
                .setRelClass(e.getRelation().getRelClass());
    }

    public String getSourceId() {
        return sourceId;
    }

    public SortableRelationKey setSourceId(String sourceId) {
        this.sourceId = sourceId;
        return this;
    }

    public String getTargetId() {
        return targetId;
    }

    public SortableRelationKey setTargetId(String targetId) {
        this.targetId = targetId;
        return this;
    }

    public String getRelType() {
        return relType;
    }

    public SortableRelationKey setRelType(String relType) {
        this.relType = relType;
        return this;
    }

    public String getSubRelType() {
        return subRelType;
    }

    public SortableRelationKey setSubRelType(String subRelType) {
        this.subRelType = subRelType;
        return this;
    }

    public String getRelClass() {
        return relClass;
    }

    public SortableRelationKey setRelClass(String relClass) {
        this.relClass = relClass;
        return this;
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
