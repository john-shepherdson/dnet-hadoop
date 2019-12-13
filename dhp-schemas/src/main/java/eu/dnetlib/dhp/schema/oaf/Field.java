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
