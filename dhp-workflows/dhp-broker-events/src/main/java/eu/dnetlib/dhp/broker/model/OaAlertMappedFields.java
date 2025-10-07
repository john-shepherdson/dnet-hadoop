
package eu.dnetlib.dhp.broker.model;

import java.io.Serializable;

public class OaAlertMappedFields implements Serializable {

	private static final long serialVersionUID = 4050762012154946651L;

	private String originalId;
	private String datasourceId;
	private String datasourceName;

	public String getOriginalId() {
		return this.originalId;
	}

	public void setOriginalId(final String originalId) {
		this.originalId = originalId;
	}

	public String getDatasourceId() {
		return this.datasourceId;
	}

	public void setDatasourceId(final String datasourceId) {
		this.datasourceId = datasourceId;
	}

	public String getDatasourceName() {
		return this.datasourceName;
	}

	public void setDatasourceName(final String datasourceName) {
		this.datasourceName = datasourceName;
	}

}
