package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class Context implements Serializable {
    private String id;

    private List<DataInfo> dataInfo;

    public String getId() {
        return id;
    }

    public Context setId(String id) {
        this.id = id;
        return this;
    }

    public List<DataInfo> getDataInfo() {
        return dataInfo;
    }

    public Context setDataInfo(List<DataInfo> dataInfo) {
        this.dataInfo = dataInfo;
        return this;
    }
}
