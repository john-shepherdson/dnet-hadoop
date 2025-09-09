package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;

public interface JoinPathAware extends Serializable {
    void setReferenceGraph(String referenceGraph);
    void setJoinEntities(String leftEntity, String rightEntity, String joinEntity, String semantics);
}
