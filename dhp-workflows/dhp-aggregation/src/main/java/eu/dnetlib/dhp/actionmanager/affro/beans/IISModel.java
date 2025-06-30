package eu.dnetlib.dhp.actionmanager.affro.beans;

import java.io.Serializable;
import java.util.List;

public class IISModel implements Serializable {
    private String id;
    private List<Author> authors;
    private List<Affiliation>affiliations;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Author> getAuthors() {
        return authors;
    }

    public void setAuthors(List<Author> authors) {
        this.authors = authors;
    }

    public List<Affiliation> getAffiliations() {
        return affiliations;
    }

    public void setAffiliations(List<Affiliation> affiliations) {
        this.affiliations = affiliations;
    }
}
