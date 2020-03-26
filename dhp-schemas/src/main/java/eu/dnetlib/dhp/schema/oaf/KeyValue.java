package eu.dnetlib.dhp.schema.oaf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

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

    public String toComparableString() {
        return isBlank()?"":String.format("%s::%s", key != null ? key.toLowerCase() : "", value != null ? value.toLowerCase() : "");
    }

    @JsonIgnore
    public boolean isBlank() {
        return StringUtils.isBlank(key) && StringUtils.isBlank(value);
    }

    @Override
    public int hashCode() {
        return toComparableString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        KeyValue other = (KeyValue) obj;

        return toComparableString().equals(other.toComparableString());
    }
}
