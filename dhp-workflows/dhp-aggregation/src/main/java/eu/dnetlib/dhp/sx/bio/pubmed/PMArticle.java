
package eu.dnetlib.dhp.sx.bio.pubmed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represent an instance of Pubmed Article extracted from the native XML
 *
 */
public class PMArticle implements Serializable {

	private String pmid;
	private String pmcId;
	private String doi;
	private String date;
	private PMJournal journal;
	private String title;
	private String description;
	private String language;
	private List<PMSubject> subjects;
	private List<PMSubject> publicationTypes = new ArrayList<>();
	private List<PMAuthor> authors = new ArrayList<>();
	private List<PMGrant> grants = new ArrayList<>();

	public String getPmid() {
		return pmid;
	}

	public void setPmid(String pmid) {
		this.pmid = pmid;
	}

	public String getPmcId() {
		return pmcId;
	}

	public void setPmcId(String pmcId) {
		this.pmcId = pmcId;
	}

	public String getDoi() {
		return doi;
	}

	public void setDoi(String doi) {
		this.doi = doi;
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

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public List<PMSubject> getSubjects() {
		return subjects;
	}

	public void setSubjects(List<PMSubject> subjects) {
		this.subjects = subjects;
	}

	public List<PMSubject> getPublicationTypes() {
		return publicationTypes;
	}

	public void setPublicationTypes(List<PMSubject> publicationTypes) {
		this.publicationTypes = publicationTypes;
	}

	public List<PMAuthor> getAuthors() {
		return authors;
	}

	public void setAuthors(List<PMAuthor> authors) {
		this.authors = authors;
	}

	public List<PMGrant> getGrants() {
		return grants;
	}

	public void setGrants(List<PMGrant> grants) {
		this.grants = grants;
	}
}
