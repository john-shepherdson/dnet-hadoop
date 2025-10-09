package eu.dnetlib.dhp.common.person;

import java.io.Serializable;

public class SerializationOrg implements Serializable {
    private String ror;
    private String openOrgs;
    private Double confidence;

    public String getRor() {
        return ror;
    }

    public void setRor(String ror) {
        this.ror = ror;
    }

    public String getOpenOrgs() {
        return openOrgs;
    }

    public void setOpenOrgs(String openOrgs) {
        this.openOrgs = openOrgs;
    }

    public Double getConfidence() {
        return confidence;
    }

    public void setConfidence(Double confidence) {
        this.confidence = confidence;
    }

}
