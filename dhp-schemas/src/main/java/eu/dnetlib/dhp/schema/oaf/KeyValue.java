package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class KeyValue implements Serializable {

    private String key;

    private String value;

    private DataInfo dataInfo;

    public String getKey() {
        return key;
    }

    public KeyValue setKey(String key) {
        this.key = key;
        return this;
    }

    public String getValue() {
        return value;
    }

    public KeyValue setValue(String value) {
        this.value = value;
        return this;
    }

    public DataInfo getDataInfo() {
        return dataInfo;
    }

    public KeyValue setDataInfo(DataInfo dataInfo) {
        this.dataInfo = dataInfo;
        return this;
    }
}
