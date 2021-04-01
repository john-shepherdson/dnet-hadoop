
package eu.dnetlib.dhp.schema.orcid;

import java.io.Serializable;

public class AuthorSummary extends OrcidData implements Serializable {
	private AuthorData authorData;
	private AuthorHistory authorHistory;

	public AuthorData getAuthorData() {
		return authorData;
	}

	public void setAuthorData(AuthorData authorData) {
		this.authorData = authorData;
	}

	public AuthorHistory getAuthorHistory() {
		return authorHistory;
	}

	public void setAuthorHistory(AuthorHistory authorHistory) {
		this.authorHistory = authorHistory;
	}
}
