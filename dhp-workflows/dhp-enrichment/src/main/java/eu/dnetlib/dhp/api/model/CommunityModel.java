
package eu.dnetlib.dhp.api.model;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import eu.dnetlib.dhp.bulktag.community.SelectionConstraints;

/**
 * @author miriam.baglioni
 * @Date 06/10/23
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CommunityModel extends CommonConfigurationModel implements Serializable {
	private String id;
	private String type;
	private String status;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}
