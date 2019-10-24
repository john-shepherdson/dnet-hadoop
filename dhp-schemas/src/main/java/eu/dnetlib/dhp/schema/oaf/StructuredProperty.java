package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class StructuredProperty implements Serializable {

    private String value;

    private Qualifier qualifier;

    private DataInfo dataInfo;

    public String getValue() {
        return value;
    }

    public StructuredProperty setValue(String value) {
        this.value = value;
        return this;
    }

    public Qualifier getQualifier() {
        return qualifier;
    }

    public StructuredProperty setQualifier(Qualifier qualifier) {
        this.qualifier = qualifier;
        return this;
    }

    public DataInfo getDataInfo() {
        return dataInfo;
    }

    public StructuredProperty setDataInfo(DataInfo dataInfo) {
        this.dataInfo = dataInfo;
        return this;
    }
}
