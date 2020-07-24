
package eu.dnetlib.dhp.schema.dump.oaf;

import java.util.List;
import java.util.Objects;

public class Context extends Qualifier {
	private List<Provenance> provenance;

	public List<Provenance> getProvenance() {
		return provenance;
	}

	public void setProvenance(List<Provenance> provenance) {
		this.provenance = provenance;
	}


	@Override
	public int hashCode() {
		String provenance = new String();
		this.provenance.forEach(p -> provenance.concat(p.toString()));
		return Objects.hash(getCode(), getLabel(), provenance);
	}



}
