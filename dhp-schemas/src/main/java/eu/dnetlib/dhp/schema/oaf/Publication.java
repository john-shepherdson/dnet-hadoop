package eu.dnetlib.dhp.schema.oaf;

import eu.dnetlib.dhp.schema.common.ModelConstants;
import java.io.Serializable;
import java.util.Objects;

public class Publication extends Result implements Serializable {

    // publication specific
    private Journal journal;

    public Publication() {
        setResulttype(ModelConstants.PUBLICATION_DEFAULT_RESULTTYPE);
    }

    public Journal getJournal() {
        return journal;
    }

    public void setJournal(Journal journal) {
        this.journal = journal;
    }

    @Override
    public void mergeFrom(OafEntity e) {
        super.mergeFrom(e);

        if (!Publication.class.isAssignableFrom(e.getClass())) {
            return;
        }

        Publication p = (Publication) e;

        if (p.getJournal() != null && compareTrust(this, e) < 0) journal = p.getJournal();
        mergeOAFDataInfo(e);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Publication that = (Publication) o;
        return Objects.equals(journal, that.journal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), journal);
    }
}
