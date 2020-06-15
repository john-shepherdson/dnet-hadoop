
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;
import java.util.Objects;

import eu.dnetlib.dhp.schema.oaf.ExtraInfo;

//ExtraInfo
public class ExternalReference implements Serializable {
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

	public static ExternalReference newInstance(ExtraInfo ei) {
		ExternalReference er = new ExternalReference();

		er.name = ei.getName();
		er.typology = ei.getTypology();
		er.provenance = ei.getProvenance();
		er.trust = ei.getTrust();
		er.value = ei.getValue();
		return er;
	}
}
