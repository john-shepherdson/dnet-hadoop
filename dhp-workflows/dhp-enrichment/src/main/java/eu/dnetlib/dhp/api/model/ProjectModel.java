
package eu.dnetlib.dhp.api.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author miriam.baglioni
 * @Date 09/10/23
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProjectModel implements Serializable {

	private String openaireId;

	public String getOpenaireId() {
		return openaireId;
	}

	public void setOpenaireId(String openaireId) {
		this.openaireId = openaireId;
	}
}
