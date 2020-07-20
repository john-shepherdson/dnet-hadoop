
package eu.dnetlib.dhp.sx.ebi.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PMArticle implements Serializable {

	private String pmid;
	private String date;
	private PMJournal journal;
	private String title;
	private String description;
	private List<PMAuthor> authors = new ArrayList<>();

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
}
