
package eu.dnetlib.dhp.oa.graph.dump.complete;

import java.io.Serializable;

public class MergedRels implements Serializable {
	private String organizationId;
	private String representativeId;

	public String getOrganizationId() {
		return organizationId;
	}

	public void setOrganizationId(String organizationId) {
		this.organizationId = organizationId;
	}

	public String getRepresentativeId() {
		return representativeId;
	}

	public void setRepresentativeId(String representativeId) {
		this.representativeId = representativeId;
	}
}
