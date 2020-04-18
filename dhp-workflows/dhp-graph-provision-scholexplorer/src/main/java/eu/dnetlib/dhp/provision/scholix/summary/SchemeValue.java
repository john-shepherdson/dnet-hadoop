package eu.dnetlib.dhp.provision.scholix.summary;

import java.io.Serializable;

public class SchemeValue implements Serializable {
    private String scheme;
    private String value;

    public SchemeValue() {}

    public SchemeValue(String scheme, String value) {
        this.scheme = scheme;
        this.value = value;
    }

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
