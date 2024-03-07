
package eu.dnetlib.dhp.countrypropagation;

import java.io.Serializable;

public class DatasourceCountry implements Serializable {
	private String dataSourceId;
	private CountrySbs country;

	public String getDataSourceId() {
		return dataSourceId;
	}

	public void setDataSourceId(String dataSourceId) {
		this.dataSourceId = dataSourceId;
	}

	public CountrySbs getCountry() {
		return country;
	}

	public void setCountry(CountrySbs country) {
		this.country = country;
	}

	public static DatasourceCountry newInstance(String dataSourceId, CountrySbs country) {
		DatasourceCountry dsc = new DatasourceCountry();
		dsc.dataSourceId = dataSourceId;
		dsc.country = country;
		return dsc;
	}
}
