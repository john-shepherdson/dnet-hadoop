
package eu.dnetlib.dhp.api.model;

import java.io.Serializable;

public class OrganizationAPIModel implements Serializable {
	private String orgId;
	private String description;
	private boolean subscribed;

	public String getOrgId() {
		return orgId;
	}

	public void setOrgId(String orgId) {
		this.orgId = orgId;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public boolean isSubscribed() {
		return subscribed;
	}

	public void setSubscribed(boolean subscribed) {
		this.subscribed = subscribed;
	}
}
