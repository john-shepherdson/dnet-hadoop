package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class StructuredProperty implements Serializable {

    private String value;

    private Qualifier qualifier;

    private DataInfo dataInfo;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Qualifier getQualifier() {
        return qualifier;
    }

    public void setQualifier(Qualifier qualifier) {
        this.qualifier = qualifier;
    }

    public DataInfo getDataInfo() {
        return dataInfo;
    }

    public void setDataInfo(DataInfo dataInfo) {
        this.dataInfo = dataInfo;
    }
}
