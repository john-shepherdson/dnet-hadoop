
package eu.dnetllib.dhp.sx.bio.pubmed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PMArticle implements Serializable {

	private String pmid;
	private String doi;
	private String date;
	private PMJournal journal;
	private String title;
	private String description;
	private String language;
	private final List<PMSubject> subjects = new ArrayList<>();
	private final List<PMSubject> publicationTypes = new ArrayList<>();
	private List<PMAuthor> authors = new ArrayList<>();

	public List<PMSubject> getPublicationTypes() {
		return publicationTypes;
	}

	private final List<PMGrant> grants = new ArrayList<>();

	public List<PMGrant> getGrants() {
		return grants;
	}

	public String getDoi() {
		return doi;
	}

	public void setDoi(String doi) {
		this.doi = doi;
	}

	public String getPmid() {
		return pmid;
	}

	public void setPmid(String pmid) {
		this.pmid = pmid;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public PMJournal getJournal() {
		return journal;
	}

	public void setJournal(PMJournal journal) {
		this.journal = journal;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<PMAuthor> getAuthors() {
		return authors;
	}

	public void setAuthors(List<PMAuthor> authors) {
		this.authors = authors;
	}

	public List<PMSubject> getSubjects() {
		return subjects;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}
}
