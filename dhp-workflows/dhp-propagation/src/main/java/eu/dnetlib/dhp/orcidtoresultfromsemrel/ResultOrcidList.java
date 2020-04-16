package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ResultWithOrcid implements Serializable {
    String id;
    List<AutoritativeAuthor> authorList = new ArrayList<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<AutoritativeAuthor> getAuthorList() {
        return authorList;
    }

    public void setAuthorList(List<AutoritativeAuthor> authorList) {
        this.authorList = authorList;
    }
}
