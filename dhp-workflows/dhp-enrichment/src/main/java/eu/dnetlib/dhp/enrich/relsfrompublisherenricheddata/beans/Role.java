package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata.beans;

import java.io.Serializable;

public class Role implements Serializable {
    private String schema;
    private String value;
    private String name;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
