package eu.dnetlib.dhp.selectioncriteria;

import java.io.Serializable;

@VerbClass("contains_ignorecase")
public class ContainsVerbIgnoreCase implements Selection, Serializable {

    private String param;

    public ContainsVerbIgnoreCase() {}

    public ContainsVerbIgnoreCase(final String param) {
        this.param = param;
    }

    @Override
    public boolean apply(String value) {
        return value.toLowerCase().contains(param.toLowerCase());
    }

    public String getParam() {
        return param;
    }

    public void setParam(String param) {
        this.param = param;
    }
}
