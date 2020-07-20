
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;
import java.util.Objects;

import eu.dnetlib.dhp.schema.oaf.ExtraInfo;

//ExtraInfo renamed ExternalReference do not confuse with ExternalReference in oaf schema
public class ExternalReference implements Serializable {
	private String name;

	private String typology;

	private Provenance provenance;

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

	public Provenance getProvenance() {
		return provenance;
	}

	public void setProvenance(Provenance provenance) {
		this.provenance = provenance;
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
		er.provenance = Provenance.newInstance(ei.getProvenance(), ei.getTrust());
		er.value = ei.getValue();
		return er;
	}
}
