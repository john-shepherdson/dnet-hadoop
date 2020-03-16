package eu.dnetlib.dhp.selectioncriteria;

@VerbClass("not_contains")
public class NotContainsVerb implements Selection {

    private String param;

    public NotContainsVerb() {
    }

    public NotContainsVerb(final String param) {
        this.param = param;
    }

    @Override
    public boolean apply(String value) {
        return !value.contains(param);
    }

    public String getParam() {
        return param;
    }

    public void setParam(String param) {
        this.param = param;
    }
}