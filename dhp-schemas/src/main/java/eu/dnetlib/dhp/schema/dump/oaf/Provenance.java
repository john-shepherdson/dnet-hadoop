
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

/**
 * Indicates the process that produced (or provided) the information, and the trust associated to the information. It
 * has two parameters: - provenance of type String to store the provenance of the information, - trust of type String to
 * store the trust associated to the information
 */
public class Provenance implements Serializable {
	private String provenance;
	private String trust;

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

	public static Provenance newInstance(String provenance, String trust) {
		Provenance p = new Provenance();
		p.provenance = provenance;
		p.trust = trust;
		return p;
	}

	public String toString() {
		return provenance + trust;
	}
}
