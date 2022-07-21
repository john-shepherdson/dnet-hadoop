package eu.dnetlib.dhp.bulktag.eosc;

import java.io.Serializable;

/**
 * @author miriam.baglioni
 * @Date 21/07/22
 */
public class DatasourceMaster implements Serializable {
    private String datasource;
    private String master;

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }
}
