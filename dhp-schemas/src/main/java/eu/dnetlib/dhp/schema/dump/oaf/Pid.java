
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

/**
 * To represent the generic persistent identifier. It has two parameters: - id of type
 * eu.dnetlib.dhp.schema.dump.oaf.ControlledField to store the scheme and value of the Persistent Identifier. -
 * provenance of type eu.dnetlib.dhp.schema.dump.oaf.Provenance to store the provenance and trust of the information
 */
public class Pid implements Serializable {
	private ControlledField id;
	private Provenance provenance;

	public ControlledField getId() {
		return id;
	}

	public void setId(ControlledField pid) {
		this.id = pid;
	}

	public Provenance getProvenance() {
		return provenance;
	}

	public void setProvenance(Provenance provenance) {
		this.provenance = provenance;
	}

	public static Pid newInstance(ControlledField pid, Provenance provenance) {
		Pid p = new Pid();
		p.id = pid;
		p.provenance = provenance;

		return p;
	}

	public static Pid newInstance(ControlledField pid) {
		Pid p = new Pid();
		p.id = pid;

		return p;
	}
}
