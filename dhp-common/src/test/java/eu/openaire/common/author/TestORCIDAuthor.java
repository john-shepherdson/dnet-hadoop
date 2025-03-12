
package eu.openaire.common.author;

import static org.apache.commons.lang3.builder.ToStringStyle.NO_CLASS_NAME_STYLE;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class TestORCIDAuthor {
	String givenName;
	String familyName;
	String creditName;
	String orcid;

	public TestORCIDAuthor(String name, String surname, String creditName, String orcid) {
		this.givenName = name;
		this.familyName = surname;
		this.creditName = creditName;
		this.orcid = orcid;
	}

	public String getFullName() {
		return givenName + " " + familyName;
	}

	public String getInvertedFullName() {
		return familyName + " " + givenName;
	}

	public String getCreditName() {
		return creditName;
	}

	public String getORCID() {
		return orcid;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this, NO_CLASS_NAME_STYLE)
			.append("givenName", givenName)
			.append("familyName", familyName)
			.append("creditName", creditName)
			.append("orcid", orcid)
			.toString();
	}
}
