package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class Field<T> implements Serializable {

    private T value;

    private DataInfo dataInfo;

    public T getValue() {
        return value;
    }

    public Field<T> setValue(T value) {
        this.value = value;
        return this;
    }

    public DataInfo getDataInfo() {
        return dataInfo;
    }

    public Field<T> setDataInfo(DataInfo dataInfo) {
        this.dataInfo = dataInfo;
        return this;
    }
}
