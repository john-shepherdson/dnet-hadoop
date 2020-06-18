
package eu.dnetlib.dhp.schema.dump.oaf;

public class Country extends Qualifier {

	private String provenance;

	public String getProvenance() {
		return provenance;
	}

	public void setProvenance(String provenance) {
		this.provenance = provenance;
	}

	public static Country newInstance(String code, String label, String provenance) {
		Country c = new Country();
		c.setProvenance(provenance);
		c.setCode(code);
		c.setLabel(label);
		return c;
	}

}
