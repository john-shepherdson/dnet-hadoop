
package eu.dnetlib.dhp.sx.bio.pubmed;

import java.io.Serializable;

/**
 * The type Pubmed author.
 *
 * @author Sandro La Bruzzo
 */
public class PMAuthor implements Serializable {

	private String lastName;
	private String foreName;

	/**
	 * Gets last name.
	 *
	 * @return the last name
	 */
	public String getLastName() {
		return lastName;
	}

	/**
	 * Sets last name.
	 *
	 * @param lastName the last name
	 */
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	/**
	 * Gets fore name.
	 *
	 * @return the fore name
	 */
	public String getForeName() {
		return foreName;
	}

	/**
	 * Sets fore name.
	 *
	 * @param foreName the fore name
	 */
	public void setForeName(String foreName) {
		this.foreName = foreName;
	}

	/**
	 * Gets full name.
	 *
	 * @return the full name
	 */
	public String getFullName() {
		return String
			.format("%s, %s", this.foreName != null ? this.foreName : "", this.lastName != null ? this.lastName : "");
	}

}
