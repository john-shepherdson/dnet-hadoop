package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class Author implements Serializable {

    private String fullname;

    private String name;

    private String surname;

    private Integer rank;

    private List<StructuredProperty> pid;

    private List<Field<String>> affiliation;

    public String getFullname() {
        return fullname;
    }

    public void setFullname(String fullname) {
        this.fullname = fullname;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public Integer getRank() {
        return rank;
    }

    public void setRank(Integer rank) {
        this.rank = rank;
    }

    public List<StructuredProperty> getPid() {
        return pid;
    }

    public void setPid(List<StructuredProperty> pid) {
        this.pid = pid;
    }

    public List<Field<String>> getAffiliation() {
        return affiliation;
    }

    public void setAffiliation(List<Field<String>> affiliation) {
        this.affiliation = affiliation;
    }
}
