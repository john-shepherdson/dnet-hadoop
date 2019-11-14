package eu.dnetlib.dhp.schema.dli;

import java.io.Serializable;

public class Subject implements Serializable {

    private String schema;

    private String value;

    public Subject() {

    }

    public Subject(String schema, String value) {
        this.schema = schema;
        this.value = value;
    }

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
}
