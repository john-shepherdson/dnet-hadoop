
package eu.dnetlib.dhp.resulttoorganizationfrominstrepo;

import java.io.Serializable;

public class DatasourceOrganization implements Serializable {

	private String datasourceId;
	private String organizationId;

	public String getDatasourceId() {
		return datasourceId;
	}

	public void setDatasourceId(String datasourceId) {
		this.datasourceId = datasourceId;
	}

	public String getOrganizationId() {
		return organizationId;
	}

	public void setOrganizationId(String organizationId) {
		this.organizationId = organizationId;
	}
}
