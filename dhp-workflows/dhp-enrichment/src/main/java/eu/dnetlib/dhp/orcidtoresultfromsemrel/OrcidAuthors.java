package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import eu.dnetlib.dhp.utils.OrcidAuthor;

import java.io.Serializable;
import java.util.List;

public class OrcidAuthors implements Serializable {
    List<OrcidAuthor> orcidAuthorList;

    public List<OrcidAuthor> getOrcidAuthorList() {
        return orcidAuthorList;
    }

    public void setOrcidAuthorList(List<OrcidAuthor> orcidAuthorList) {
        this.orcidAuthorList = orcidAuthorList;
    }
}
