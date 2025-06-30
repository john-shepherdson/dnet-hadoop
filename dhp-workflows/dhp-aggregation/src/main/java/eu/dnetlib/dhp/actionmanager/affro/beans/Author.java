package eu.dnetlib.dhp.actionmanager.affro.beans;

import java.io.Serializable;
import java.util.List;


public class Author implements Serializable {
    private String authorfullname;
    private List<Integer> affiliationpositions;

    public String getAuthorfullname() {
        return authorfullname;
    }

    public void setAuthorfullname(String authorfullname) {
        this.authorfullname = authorfullname;
    }

    public List<Integer> getAffiliationpositions() {
        return affiliationpositions;
    }

    public void setAffiliationpositions(List<Integer> affiliationpositions) {
        this.affiliationpositions = affiliationpositions;
    }
}
