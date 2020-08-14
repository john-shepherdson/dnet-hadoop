package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

public class Project implements Serializable {
    protected String id;// OpenAIRE id
    protected String code;

    protected String acronym;

    protected String title;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getAcronym() {
        return acronym;
    }

    public void setAcronym(String acronym) {
        this.acronym = acronym;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
