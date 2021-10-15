
package eu.dnetlib.dhp.sx.bio.pubmed;

public class PMGrant {

	private String grantID;
	private String agency;
	private String country;

	public PMGrant() {
	}

	public PMGrant(String grantID, String agency, String country) {
		this.grantID = grantID;
		this.agency = agency;
		this.country = country;
	}

	public String getGrantID() {
		return grantID;
	}

	public void setGrantID(String grantID) {
		this.grantID = grantID;
	}

	public String getAgency() {
		return agency;
	}

	public void setAgency(String agency) {
		this.agency = agency;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}
}
