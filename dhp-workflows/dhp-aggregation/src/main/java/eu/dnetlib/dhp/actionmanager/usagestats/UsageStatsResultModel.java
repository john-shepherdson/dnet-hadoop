
package eu.dnetlib.dhp.actionmanager.usagestats;

/**
 * @author miriam.baglioni
 * @Date 30/06/23
 */
public class UsageStatsResultModel extends UsageStatsModel {
	private String datasourceId;

	public String getDatasourceId() {
		return datasourceId;
	}

	public void setDatasourceId(String datasourceId) {
		this.datasourceId = datasourceId;
	}
}
