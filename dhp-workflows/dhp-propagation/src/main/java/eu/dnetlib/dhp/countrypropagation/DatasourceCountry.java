package eu.dnetlib.dhp.countrypropagation;

import java.io.Serializable;

public class DatasourceCountry implements Serializable {
    private String dataSourceId;
    private String country;

    public String getDataSourceId() {
        return dataSourceId;
    }

    public void setDataSourceId(String dataSourceId) {
        this.dataSourceId = dataSourceId;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }
}
