package eu.dnetlib.dhp.schema.dli;

import java.io.Serializable;
import java.util.List;

public class Relation implements Serializable {

    private String source;

    private String target;

    private List<Provenance> provenance;

    private RelationSemantic semantic;

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

    public List<Provenance> getProvenance() {
        return provenance;
    }

    public void setProvenance(List<Provenance> provenance) {
        this.provenance = provenance;
    }

    public RelationSemantic getSemantic() {
        return semantic;
    }

    public void setSemantic(RelationSemantic semantic) {
        this.semantic = semantic;
    }
}
