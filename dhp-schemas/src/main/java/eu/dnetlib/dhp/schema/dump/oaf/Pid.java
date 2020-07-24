
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

public class Pid implements Serializable {
	private ControlledField pid;
	private Provenance provenance;

	public ControlledField getPid() {
		return pid;
	}

	public void setPid(ControlledField pid) {
		this.pid = pid;
	}

	public Provenance getProvenance() {
		return provenance;
	}

	public void setProvenance(Provenance provenance) {
		this.provenance = provenance;
	}
}
