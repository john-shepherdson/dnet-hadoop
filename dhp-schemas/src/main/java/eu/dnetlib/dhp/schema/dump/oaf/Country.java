
package eu.dnetlib.dhp.schema.dump.oaf;

/**
 * Represents the country associated to this result. It extends eu.dnetlib.dhp.schema.dump.oaf.Qualifier with a
 * provenance parameter of type eu.dnetlib.dhp.schema.dumo.oaf.Provenance. The country in not mapped if its value in
 * the result reprensented in the internal format is Unknown. The value for this element correspond to:
 *  - code corresponds to the classid of eu.dnetlib.dhp.schema.oaf.Country
 *  - label corresponds to the classname of eu.dnetlib.dhp.schema.oaf.Country
 *  - provenance set only if the dataInfo associated to the Country of the result to be dumped is not null. In this case :
 *    - provenance corresponds to dataInfo.provenanceaction.classid (to be modified with datainfo.provenanceaction.classname)
 *    - trust corresponds to dataInfo.trust
 */
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

	public static Country newInstance(String code, String label, String provenance, String trust) {
		return newInstance(code, label, Provenance.newInstance(provenance, trust));
	}

}
