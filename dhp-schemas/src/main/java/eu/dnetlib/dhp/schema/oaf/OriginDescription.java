package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class OriginDescription implements Serializable {

    private String harvestDate;

    private Boolean altered = true;

    private String baseURL;

    private String identifier;

    private String datestamp;

    private String metadataNamespace;

    public String getHarvestDate() {
        return harvestDate;
    }

    public void setHarvestDate(String harvestDate) {
        this.harvestDate = harvestDate;
    }

    public Boolean getAltered() {
        return altered;
    }

    public void setAltered(Boolean altered) {
        this.altered = altered;
    }

    public String getBaseURL() {
        return baseURL;
    }

    public void setBaseURL(String baseURL) {
        this.baseURL = baseURL;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getDatestamp() {
        return datestamp;
    }

    public void setDatestamp(String datestamp) {
        this.datestamp = datestamp;
    }

    public String getMetadataNamespace() {
        return metadataNamespace;
    }

    public void setMetadataNamespace(String metadataNamespace) {
        this.metadataNamespace = metadataNamespace;
    }
}
