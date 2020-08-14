
package eu.dnetlib.dhp.common.api.zenodo;

public class Creator {
	private String affiliation;
	private String name;
	private String orcid;

	public String getAffiliation() {
		return affiliation;
	}

	public void setAffiliation(String affiliation) {
		this.affiliation = affiliation;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getOrcid() {
		return orcid;
	}

	public void setOrcid(String orcid) {
		this.orcid = orcid;
	}

	public static Creator newInstance(String name, String affiliation, String orcid) {
		Creator c = new Creator();
		if (!(name == null)) {
			c.name = name;
		}
		if (!(affiliation == null)) {
			c.affiliation = affiliation;
		}
		if (!(orcid == null)) {
			c.orcid = orcid;
		}

		return c;
	}
}
