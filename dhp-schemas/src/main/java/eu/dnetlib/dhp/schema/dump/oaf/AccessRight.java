package eu.dnetlib.dhp.schema.dump.oaf;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AccessRight extends Qualifier{

    private String schema;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }


}
