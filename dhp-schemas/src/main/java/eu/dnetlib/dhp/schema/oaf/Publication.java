package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class Publication extends Result implements Serializable {

    // publication specific
    private Journal journal;

    public Journal getJournal() {
        return journal;
    }

    public void setJournal(Journal journal) {
        this.journal = journal;
    }
}
