package eu.dnetlib.dhp.schema.dump.oaf.graph;

import eu.dnetlib.dhp.schema.dump.oaf.Provenance;

import java.io.Serializable;

public class Relation implements Serializable {
    private Node source;
    private Node target;
    private RelType reltype;
    private Provenance provenance;

    public Node getSource() {
        return source;
    }

    public void setSource(Node source) {
        this.source = source;
    }

    public Node getTarget() {
        return target;
    }

    public void setTarget(Node target) {
        this.target = target;
    }

    public RelType getReltype() {
        return reltype;
    }

    public void setReltype(RelType reltype) {
        this.reltype = reltype;
    }

    public Provenance getProvenance() {
        return provenance;
    }

    public void setProvenance(Provenance provenance) {
        this.provenance = provenance;
    }
}
