
package eu.dnetlib.dhp.sx.bio.pubmed;

import java.io.Serializable;

/**
 * The type Pm journal.
 *
 * @author  Sandro La Bruzzo
 */
public class PMJournal implements Serializable {

	private String issn;
	private String volume;
	private String issue;
	private String date;
	private String title;

	/**
	 * Gets issn.
	 *
	 * @return the issn
	 */
	public String getIssn() {
		return issn;
	}

	/**
	 * Sets issn.
	 *
	 * @param issn the issn
	 */
	public void setIssn(String issn) {
		this.issn = issn;
	}

	/**
	 * Gets volume.
	 *
	 * @return the volume
	 */
	public String getVolume() {
		return volume;
	}

	/**
	 * Sets volume.
	 *
	 * @param volume the volume
	 */
	public void setVolume(String volume) {
		this.volume = volume;
	}

	/**
	 * Gets issue.
	 *
	 * @return the issue
	 */
	public String getIssue() {
		return issue;
	}

	/**
	 * Sets issue.
	 *
	 * @param issue the issue
	 */
	public void setIssue(String issue) {
		this.issue = issue;
	}

	/**
	 * Gets date.
	 *
	 * @return the date
	 */
	public String getDate() {
		return date;
	}

	/**
	 * Sets date.
	 *
	 * @param date the date
	 */
	public void setDate(String date) {
		this.date = date;
	}

	/**
	 * Gets title.
	 *
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * Sets title.
	 *
	 * @param title the title
	 */
	public void setTitle(String title) {
		this.title = title;
	}
}
