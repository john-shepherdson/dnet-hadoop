package eu.dnetlib.dhp.provision.scholix;

import java.io.Serializable;

public class ScholixRelationship implements Serializable {
    private String name;
    private String schema;
    private String inverse;

    public ScholixRelationship() {}

    public ScholixRelationship(String name, String schema, String inverse) {
        this.name = name;
        this.schema = schema;
        this.inverse = inverse;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getInverse() {
        return inverse;
    }

    public void setInverse(String inverse) {
        this.inverse = inverse;
    }
}
