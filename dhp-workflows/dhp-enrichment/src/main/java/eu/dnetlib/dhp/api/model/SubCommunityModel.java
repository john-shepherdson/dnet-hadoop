package eu.dnetlib.dhp.api.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SubCommunityModel extends CommonConfigurationModel implements Serializable {
    private String subCommunityId;

    public String getSubCommunityId() {
        return subCommunityId;
    }

    public void setSubCommunityId(String subCommunityId) {
        this.subCommunityId = subCommunityId;
    }
}
