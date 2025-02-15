
package eu.dnetlib.dhp.api.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

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
