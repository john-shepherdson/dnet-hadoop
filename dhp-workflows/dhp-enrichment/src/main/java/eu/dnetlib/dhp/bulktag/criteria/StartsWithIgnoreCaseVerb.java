package eu.dnetlib.dhp.bulktag.criteria;

import java.io.Serializable;


/**
 * @author miriam.baglioni
 * @Date 06/04/23
 */

@VerbClass("starts_with_caseinsensitive")
public class StartsWithIgnoreCaseVerb implements Selection, Serializable {
    private String param;

    public StartsWithIgnoreCaseVerb() {
    }

    public StartsWithIgnoreCaseVerb(final String param) {
        this.param = param;
    }

    @Override
    public boolean apply(String value) {
        return value.toLowerCase().startsWith(param.toLowerCase());
    }

    public String getParam() {
        return param;
    }

    public void setParam(String param) {
        this.param = param;
    }
}
