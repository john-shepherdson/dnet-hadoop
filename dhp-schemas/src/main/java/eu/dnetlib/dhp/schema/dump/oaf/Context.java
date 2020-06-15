
package eu.dnetlib.dhp.schema.dump.oaf;

import java.util.List;

public class Context extends Qualifier {
	private List<String> provenance;

	public List<String> getProvenance() {
		return provenance;
	}

	public void setProvenance(List<String> provenance) {
		this.provenance = provenance;
	}
}
