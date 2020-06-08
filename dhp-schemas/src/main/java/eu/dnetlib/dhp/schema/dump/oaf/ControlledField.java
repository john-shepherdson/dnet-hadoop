package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

public class ControlledField implements Serializable {
    private String scheme;
    private String value;

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
