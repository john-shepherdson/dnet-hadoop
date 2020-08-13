
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

/**
 * To represent keywords associated to the result. It has two parameters:
 * - subject of type eu.dnetlib.dhp.schema.dump.oaf.ControlledField to describe the subject. It mapped as:
 *   - schema it corresponds to qualifier.classid of the dumped subject
 *   - value it corresponds to the subject value
 * - provenance of type eu.dnetlib.dhp.schema.dump.oaf.Provenance to represent the provenance of the subject.
 *   It is dumped only if dataInfo is not null. In this case:
 *   - provenance corresponds to dataInfo.provenanceaction.classname
 *   - trust corresponds to dataInfo.trust
 */
public class Subject implements Serializable {
	private ControlledField subject;
	private Provenance provenance;

	public ControlledField getSubject() {
		return subject;
	}

	public void setSubject(ControlledField subject) {
		this.subject = subject;
	}

	public Provenance getProvenance() {
		return provenance;
	}

	public void setProvenance(Provenance provenance) {
		this.provenance = provenance;
	}

}
