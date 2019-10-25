package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

public class Publication extends Result<Publication> implements Serializable {

    // publication specific
    private Journal journal;

    public Journal getJournal() {
        return journal;
    }

    public Publication setJournal(Journal journal) {
        this.journal = journal;
        return this;
    }

    @Override
    protected Publication self() {
        return self();
    }
}
