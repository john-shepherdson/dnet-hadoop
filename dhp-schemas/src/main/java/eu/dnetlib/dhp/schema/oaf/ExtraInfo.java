package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class ExtraInfo implements Serializable {
    private String name;

    private String typology;

    private String provenance;

    private String trust;

    // json containing a Citation or Statistics
    private String value;

    public String getName() {
        return name;
    }

    public ExtraInfo setName(String name) {
        this.name = name;
        return this;
    }

    public String getTypology() {
        return typology;
    }

    public ExtraInfo setTypology(String typology) {
        this.typology = typology;
        return this;
    }

    public String getProvenance() {
        return provenance;
    }

    public ExtraInfo setProvenance(String provenance) {
        this.provenance = provenance;
        return this;
    }

    public String getTrust() {
        return trust;
    }

    public ExtraInfo setTrust(String trust) {
        this.trust = trust;
        return this;
    }

    public String getValue() {
        return value;
    }

    public ExtraInfo setValue(String value) {
        this.value = value;
        return this;
    }
}
