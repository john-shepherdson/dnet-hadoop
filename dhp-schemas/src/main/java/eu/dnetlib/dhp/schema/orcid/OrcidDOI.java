
package eu.dnetlib.dhp.schema.orcid;

import java.util.List;

public class OrcidDOI {
	private String doi;
	private List<AuthorData> authors;

	public String getDoi() {
		return doi;
	}

	public void setDoi(String doi) {
		this.doi = doi;
	}

	public List<AuthorData> getAuthors() {
		return authors;
	}

	public void setAuthors(List<AuthorData> authors) {
		this.authors = authors;
	}
}
