package eu.dnetlib.dhp.actionmanager.bipfinder;

import java.io.Serializable;

public class PreparedResult implements Serializable {
    private String id; //openaire id
    private String value; //doi

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
