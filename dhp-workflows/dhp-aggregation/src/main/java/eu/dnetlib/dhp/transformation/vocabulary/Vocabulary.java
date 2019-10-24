package eu.dnetlib.dhp.transformation.vocabulary;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class Vocabulary implements Serializable {

    private String id;
    private String name;
    private String description;
    private String code;
    private List<Term> terms;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public List<Term> getTerms() {
        return terms;
    }

    public void setTerms(List<Term> terms) {
        this.terms = terms;
    }



}
