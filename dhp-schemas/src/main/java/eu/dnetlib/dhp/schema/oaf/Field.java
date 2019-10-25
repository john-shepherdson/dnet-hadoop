package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class Field<T> implements Serializable {

    private T value;

    private DataInfo dataInfo;

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public DataInfo getDataInfo() {
        return dataInfo;
    }

    public void setDataInfo(DataInfo dataInfo) {
        this.dataInfo = dataInfo;
    }
}
