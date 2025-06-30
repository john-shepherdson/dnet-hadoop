package eu.dnetlib.dhp.actionmanager.affro.beans;

import java.io.Serializable;

public class Affiliation implements Serializable {
    private String rawtext;

    public String getRawtext() {
        return rawtext;
    }

    public void setRawtext(String rawtext) {
        this.rawtext = rawtext;
    }
}
