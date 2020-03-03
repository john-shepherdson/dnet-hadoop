package eu.dnetlib.dhp.selectioncriteria;


@VerbClass("not_equals")
public class NotEqualVerb implements Selection {

    private String param;


    public NotEqualVerb(final String param) {
        this.param = param;
    }

    public NotEqualVerb() {
    }

    public String getParam() {
        return param;
    }

    public void setParam(String param) {
        this.param = param;
    }

    @Override
    public boolean apply(String value) {
        return !value.equalsIgnoreCase(param);
    }
}