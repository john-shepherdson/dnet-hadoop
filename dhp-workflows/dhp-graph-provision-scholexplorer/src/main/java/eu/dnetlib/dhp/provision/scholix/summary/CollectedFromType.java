
package eu.dnetlib.dhp.provision.scholix.summary;

import java.io.Serializable;

public class CollectedFromType implements Serializable {

	private String datasourceName;
	private String datasourceId;
	private String completionStatus;

	public CollectedFromType() {
	}

	public CollectedFromType(String datasourceName, String datasourceId, String completionStatus) {
		this.datasourceName = datasourceName;
		this.datasourceId = datasourceId;
		this.completionStatus = completionStatus;
	}

	public String getDatasourceName() {
		return datasourceName;
	}

	public void setDatasourceName(String datasourceName) {
		this.datasourceName = datasourceName;
	}

	public String getDatasourceId() {
		return datasourceId;
	}

	public void setDatasourceId(String datasourceId) {
		this.datasourceId = datasourceId;
	}

	public String getCompletionStatus() {
		return completionStatus;
	}

	public void setCompletionStatus(String completionStatus) {
		this.completionStatus = completionStatus;
	}
}
