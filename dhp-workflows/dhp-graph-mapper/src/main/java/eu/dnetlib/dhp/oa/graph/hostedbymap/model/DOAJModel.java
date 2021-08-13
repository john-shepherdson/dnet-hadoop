
package eu.dnetlib.dhp.oa.graph.hostedbymap.model;

import java.io.Serializable;

import com.opencsv.bean.CsvBindByName;

public class DOAJModel implements Serializable {

	@CsvBindByName(column = "Journal title")
	private String journalTitle;

	@CsvBindByName(column = "Journal ISSN (print version)")
	private String issn;

	@CsvBindByName(column = "Journal EISSN (online version)")
	private String eissn;

	@CsvBindByName(column = "Review process")
	private String reviewProcess;

	public String getJournalTitle() {
		return journalTitle;
	}

	public void setJournalTitle(String journalTitle) {
		this.journalTitle = journalTitle;
	}

	public String getIssn() {
		return issn;
	}

	public void setIssn(String issn) {
		this.issn = issn;
	}

	public String getEissn() {
		return eissn;
	}

	public void setEissn(String eissn) {
		this.eissn = eissn;
	}

	public String getReviewProcess() {
		return reviewProcess;
	}

	public void setReviewProcess(String reviewProcess) {
		this.reviewProcess = reviewProcess;
	}
}
