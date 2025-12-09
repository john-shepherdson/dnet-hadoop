package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata.beans;

import java.io.Serializable;

public class Pid implements Serializable {
         private String schema;
         private String value;

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
