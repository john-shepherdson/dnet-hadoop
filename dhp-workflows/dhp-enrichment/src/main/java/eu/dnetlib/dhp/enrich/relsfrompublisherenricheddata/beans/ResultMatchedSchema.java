package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata.beans;

import java.io.Serializable;
import java.util.List;


public class ResultMatchedSchema implements Serializable {
    private String id;
    private List<Author> authors;
    private List<Matching> organizations;

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

    public List<Matching> getOrganizations() {
        return organizations;
    }

    public void setOrganizations(List<Matching> organizations) {
        this.organizations = organizations;
    }
}


