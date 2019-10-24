package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class DataInfo implements Serializable {

    private Boolean invisible = false;
    private Boolean inferred;
    private Boolean deletedbyinference;
    private String trust;
    private String inferenceprovenance;
    private Qualifier provenanceaction;


    public Boolean getInvisible() {
        return invisible;
    }

    public DataInfo setInvisible(Boolean invisible) {
        this.invisible = invisible;
        return this;
    }

    public Boolean getInferred() {
        return inferred;
    }

    public DataInfo setInferred(Boolean inferred) {
        this.inferred = inferred;
        return this;
    }

    public Boolean getDeletedbyinference() {
        return deletedbyinference;
    }

    public DataInfo setDeletedbyinference(Boolean deletedbyinference) {
        this.deletedbyinference = deletedbyinference;
        return this;
    }

    public String getTrust() {
        return trust;
    }

    public DataInfo setTrust(String trust) {
        this.trust = trust;
        return this;
    }

    public String getInferenceprovenance() {
        return inferenceprovenance;
    }

    public DataInfo setInferenceprovenance(String inferenceprovenance) {
        this.inferenceprovenance = inferenceprovenance;
        return this;
    }

    public Qualifier getProvenanceaction() {
        return provenanceaction;
    }

    public DataInfo setProvenanceaction(Qualifier provenanceaction) {
        this.provenanceaction = provenanceaction;
        return this;
    }
}
