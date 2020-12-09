
package eu.dnetlib.dhp.schema.orcid;

public class AuthorSummary extends OrcidData {
	AuthorData authorData;
	AuthorHistory authorHistory;

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
