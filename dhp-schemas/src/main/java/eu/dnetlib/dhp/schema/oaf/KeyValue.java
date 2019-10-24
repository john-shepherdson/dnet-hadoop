package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class KeyValue implements Serializable {

    private String key;

    private String value;

    private DataInfo dataInfo;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public DataInfo getDataInfo() {
        return dataInfo;
    }

    public void setDataInfo(DataInfo dataInfo) {
        this.dataInfo = dataInfo;
    }
}
