package eu.dnetlib.dhp.countrypropagation;

import java.io.Serializable;

public class DatasourceCountry implements Serializable {
    private String dataSourceId;
    private Country country;

    public String getDataSourceId() {
        return dataSourceId;
    }

    public void setDataSourceId(String dataSourceId) {
        this.dataSourceId = dataSourceId;
    }

    public Country getCountry() {
        return country;
    }

    public void setCountry(Country country) {
        this.country = country;
    }
}
