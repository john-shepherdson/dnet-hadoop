
package eu.dnetlib.dhp.schema.dump.oaf;

public class Country extends Qualifier {

	private Provenance provenance;

	public Provenance getProvenance() {
		return provenance;
	}

	public void setProvenance(Provenance provenance) {
		this.provenance = provenance;
	}

	public static Country newInstance(String code, String label, Provenance provenance) {
		Country c = new Country();
		c.setProvenance(provenance);
		c.setCode(code);
		c.setLabel(label);
		return c;
	}

	public static Country newInstance(String code, String label, String provenance, String trust){
		return newInstance(code, label, Provenance.newInstance(provenance, trust));
	}

}
