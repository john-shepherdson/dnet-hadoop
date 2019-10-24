package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class Context implements Serializable {
    private String id;

    private List<DataInfo> dataInfo;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<DataInfo> getDataInfo() {
        return dataInfo;
    }

    public void setDataInfo(List<DataInfo> dataInfo) {
        this.dataInfo = dataInfo;
    }
}
