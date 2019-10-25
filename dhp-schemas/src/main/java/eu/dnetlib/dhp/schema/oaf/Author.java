package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.List;

public class Author implements Serializable {

    private String fullname;

    private String name;

    private String surname;

    private Integer rank;

    private List<KeyValue> pid;

    private List<Field<String>> affiliation;

    public String getFullname() {
        return fullname;
    }

    public Author setFullname(String fullname) {
        this.fullname = fullname;
        return this;
    }

    public String getName() {
        return name;
    }

    public Author setName(String name) {
        this.name = name;
        return this;
    }

    public String getSurname() {
        return surname;
    }

    public Author setSurname(String surname) {
        this.surname = surname;
        return this;
    }

    public Integer getRank() {
        return rank;
    }

    public Author setRank(Integer rank) {
        this.rank = rank;
        return this;
    }

    public List<KeyValue> getPid() {
        return pid;
    }

    public Author setPid(List<KeyValue> pid) {
        this.pid = pid;
        return this;
    }

    public List<Field<String>> getAffiliation() {
        return affiliation;
    }

    public Author setAffiliation(List<Field<String>> affiliation) {
        this.affiliation = affiliation;
        return this;
    }
}
