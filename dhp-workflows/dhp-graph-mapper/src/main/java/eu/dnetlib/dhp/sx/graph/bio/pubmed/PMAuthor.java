
package eu.dnetlib.dhp.sx.graph.bio.pubmed;

import java.io.Serializable;

public class PMAuthor implements Serializable {

	private String lastName;
	private String foreName;

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getForeName() {
		return foreName;
	}

	public void setForeName(String foreName) {
		this.foreName = foreName;
	}

	public String getFullName() {
		return String
			.format("%s, %s", this.foreName != null ? this.foreName : "", this.lastName != null ? this.lastName : "");
	}

}
