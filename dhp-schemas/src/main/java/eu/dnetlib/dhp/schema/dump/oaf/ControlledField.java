package eu.dnetlib.dhp.schema.dump.oaf;

import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

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

    public static ControlledField newInstance(StructuredProperty pid){
            ControlledField cf = new ControlledField();

        cf.scheme = pid.getQualifier().getClassid();
        cf.value = pid.getValue();

        return cf;
    }
}
