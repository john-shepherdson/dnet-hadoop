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

    @Override
    public int hashCode() {
        return getValue() == null ? 0 : getValue().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Field<T> other = (Field<T>) obj;
        return getValue().equals(other.getValue());
    }
}
