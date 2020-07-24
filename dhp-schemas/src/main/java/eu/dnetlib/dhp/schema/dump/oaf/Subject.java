
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

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
