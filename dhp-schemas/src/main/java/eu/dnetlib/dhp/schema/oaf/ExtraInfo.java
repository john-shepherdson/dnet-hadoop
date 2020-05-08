
package eu.dnetlib.dhp.schema.oaf;

import java.io.Serializable;
import java.util.Objects;

public class ExtraInfo implements Serializable {
	private String name;

	private String typology;

	private String provenance;

	private String trust;

	// json containing a Citation or Statistics
	private String value;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTypology() {
		return typology;
	}

	public void setTypology(String typology) {
		this.typology = typology;
	}

	public String getProvenance() {
		return provenance;
	}

	public void setProvenance(String provenance) {
		this.provenance = provenance;
	}

	public String getTrust() {
		return trust;
	}

	public void setTrust(String trust) {
		this.trust = trust;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		ExtraInfo extraInfo = (ExtraInfo) o;
		return Objects.equals(name, extraInfo.name)
			&& Objects.equals(typology, extraInfo.typology)
			&& Objects.equals(provenance, extraInfo.provenance)
			&& Objects.equals(trust, extraInfo.trust)
			&& Objects.equals(value, extraInfo.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, typology, provenance, trust, value);
	}
}
