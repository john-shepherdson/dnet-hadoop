package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class OriginDescription implements Serializable {

    private String harvestDate;

    private Boolean altered = true;

    private String baseURL;

    private String identifier;

    private String datestamp;

    private String metadataNamespace;

    //private OriginDescription originDescription;

    public String getHarvestDate() {
        return harvestDate;
    }

    public OriginDescription setHarvestDate(String harvestDate) {
        this.harvestDate = harvestDate;
        return this;
    }

    public Boolean getAltered() {
        return altered;
    }

    public OriginDescription setAltered(Boolean altered) {
        this.altered = altered;
        return this;
    }

    public String getBaseURL() {
        return baseURL;
    }

    public OriginDescription setBaseURL(String baseURL) {
        this.baseURL = baseURL;
        return this;
    }

    public String getIdentifier() {
        return identifier;
    }

    public OriginDescription setIdentifier(String identifier) {
        this.identifier = identifier;
        return this;
    }

    public String getDatestamp() {
        return datestamp;
    }

    public OriginDescription setDatestamp(String datestamp) {
        this.datestamp = datestamp;
        return this;
    }

    public String getMetadataNamespace() {
        return metadataNamespace;
    }

    public OriginDescription setMetadataNamespace(String metadataNamespace) {
        this.metadataNamespace = metadataNamespace;
        return this;
    }

//    public OriginDescription getOriginDescription() {
//        return originDescription;
//    }
//
//    public OriginDescription setOriginDescription(OriginDescription originDescription) {
//        this.originDescription = originDescription;
//        return this;
//    }
}
