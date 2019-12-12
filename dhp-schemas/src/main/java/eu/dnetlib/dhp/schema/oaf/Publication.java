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

    @Override
    public void mergeFrom(OafEntity e) {
        super.mergeFrom(e);

        Publication p = (Publication) e;

        if (p.getJournal() != null && compareTrust(this, e)<0)
            journal = p.getJournal();
        mergeOAFDataInfo(e);
    }


}
