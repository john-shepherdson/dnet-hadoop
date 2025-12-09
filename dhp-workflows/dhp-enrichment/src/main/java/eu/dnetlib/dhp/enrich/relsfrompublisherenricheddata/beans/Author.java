package eu.dnetlib.dhp.enrich.relsfrompublisherenricheddata.beans;

import java.io.Serializable;
import java.util.List;

public class Author implements Serializable {
    private String fullname;
    private String firstname;
    private String lastname;
    private List<Affiliation> affiliations;
    private Boolean corresponding;
    private List<Role> contributor_roles;
    private List<Pid> pids;


    public String getFullname() {
        return fullname;
    }

    public void setFullname(String fullname) {
        this.fullname = fullname;
    }

    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    public List<Affiliation> getAffiliations() {
        return affiliations;
    }

    public void setAffiliations(List<Affiliation> affiliations) {
        this.affiliations = affiliations;
    }

    public Boolean getCorresponding() {
        return corresponding;
    }

    public void setCorresponding(Boolean corresponding) {
        this.corresponding = corresponding;
    }

    public List<Role> getContributor_roles() {
        return contributor_roles;
    }

    public void setContributor_roles(List<Role> contributor_roles) {
        this.contributor_roles = contributor_roles;
    }

    public List<Pid> getPids() {
        return pids;
    }

    public void setPids(List<Pid> pids) {
        this.pids = pids;
    }
}
