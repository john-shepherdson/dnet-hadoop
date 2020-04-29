
package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;

import eu.dnetlib.dhp.schema.common.ModelConstants;

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

		if (p.getJournal() != null && compareTrust(this, e) < 0)
			journal = p.getJournal();
		mergeOAFDataInfo(e);
	}
}
