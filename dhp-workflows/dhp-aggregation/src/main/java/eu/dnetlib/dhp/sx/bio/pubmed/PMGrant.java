
package eu.dnetlib.dhp.sx.bio.pubmed;

/**
 * The type Pm grant.
 *
 * @author Sandro La Bruzzo
 */
public class PMGrant {

	private String grantID;
	private String agency;
	private String country;

	/**
	 * Instantiates a new Pm grant.
	 */
	public PMGrant() {
	}

	/**
	 * Instantiates a new Pm grant.
	 *
	 * @param grantID the grant id
	 * @param agency  the agency
	 * @param country the country
	 */
	public PMGrant(String grantID, String agency, String country) {
		this.grantID = grantID;
		this.agency = agency;
		this.country = country;
	}

	/**
	 * Gets grant id.
	 *
	 * @return the grant id
	 */
	public String getGrantID() {
		return grantID;
	}

	/**
	 * Sets grant id.
	 *
	 * @param grantID the grant id
	 */
	public void setGrantID(String grantID) {
		this.grantID = grantID;
	}

	/**
	 * Gets agency.
	 *
	 * @return the agency
	 */
	public String getAgency() {
		return agency;
	}

	/**
	 * Sets agency.
	 *
	 * @param agency the agency
	 */
	public void setAgency(String agency) {
		this.agency = agency;
	}

	/**
	 * Gets country.
	 *
	 * @return the country
	 */
	public String getCountry() {
		return country;
	}

	/**
	 * Sets country.
	 *
	 * @param country the country
	 */
	public void setCountry(String country) {
		this.country = country;
	}
}
