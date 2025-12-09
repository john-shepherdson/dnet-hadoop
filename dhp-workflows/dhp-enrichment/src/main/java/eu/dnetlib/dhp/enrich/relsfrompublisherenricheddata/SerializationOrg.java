package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata;

import java.io.Serializable;

public class SerializationOrg implements Serializable {
    private String ror;
    private String openOrgs;
    private Double confidence;
    private String name;
    private String country;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

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
