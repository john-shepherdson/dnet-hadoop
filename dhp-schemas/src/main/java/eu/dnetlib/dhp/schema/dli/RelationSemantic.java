package eu.dnetlib.dhp.schema.dli;

import java.io.Serializable;

public class RelationSemantic extends Subject implements Serializable {

    public String inverse;

    public String getInverse() {
        return inverse;
    }

    public void setInverse(String inverse) {
        this.inverse = inverse;
    }
}
