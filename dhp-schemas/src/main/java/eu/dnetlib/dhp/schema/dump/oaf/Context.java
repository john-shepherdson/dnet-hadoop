
package eu.dnetlib.dhp.schema.dump.oaf;

import java.util.List;

public class Context extends Qualifier {
	private List<Provenance> provenance;

	public List<Provenance> getProvenance() {
		return provenance;
	}

	public void setProvenance(List<Provenance> provenance) {
		this.provenance = provenance;
	}
}
