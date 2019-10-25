package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class Author implements Serializable {

    private String name;

    private String typology;

    private String provenance;

    private String trust;

    // json containing a Citation or Statistics
    private String value;

    public String getName() {
        return name;
    }

    public Author setName(String name) {
        this.name = name;
        return this;
    }

    public String getTypology() {
        return typology;
    }

    public Author setTypology(String typology) {
        this.typology = typology;
        return this;
    }

    public String getProvenance() {
        return provenance;
    }

    public Author setProvenance(String provenance) {
        this.provenance = provenance;
        return this;
    }

    public String getTrust() {
        return trust;
    }

    public Author setTrust(String trust) {
        this.trust = trust;
        return this;
    }

    public String getValue() {
        return value;
    }

    public Author setValue(String value) {
        this.value = value;
        return this;
    }
}
