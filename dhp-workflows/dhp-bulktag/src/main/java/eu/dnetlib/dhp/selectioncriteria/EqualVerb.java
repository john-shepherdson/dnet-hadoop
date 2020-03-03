package eu.dnetlib.dhp.selectioncriteria;


@VerbClass("equals")
public class EqualVerb implements Selection {

    private String param;

    public EqualVerb() {
    }

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