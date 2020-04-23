package eu.dnetlib.dhp.selectioncriteria;

import java.io.Serializable;

@VerbClass("equals")
public class EqualVerb implements Selection, Serializable {

    private String param;

    public EqualVerb() {}

    public EqualVerb(final String param) {
        this.param = param;
    }

    @Override
    public boolean apply(String value) {
        return value.equalsIgnoreCase(param);
    }

    public String getParam() {
        return param;
    }

    public void setParam(String param) {
        this.param = param;
    }
}
