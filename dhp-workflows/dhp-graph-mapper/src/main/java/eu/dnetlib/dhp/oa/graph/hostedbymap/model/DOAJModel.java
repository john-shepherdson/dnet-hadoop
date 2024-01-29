
package eu.dnetlib.dhp.oa.graph.hostedbymap.model;

import java.io.Serializable;
import java.util.List;

import com.opencsv.bean.CsvBindByName;

public class DOAJModel implements Serializable {

	@CsvBindByName(column = "Journal title")
	private String journalTitle;

	@CsvBindByName(column = "Journal ISSN (print version)")
	private String issn;

	@CsvBindByName(column = "Journal EISSN (online version)")
	private String eissn;

	@CsvBindByName(column = "Review process")
	private List<String> reviewProcess;

	private Integer oaStart;

	public Integer getOaStart() {
		return oaStart;
	}

	public void setOaStart(Integer oaStart) {
		this.oaStart = oaStart;
	}

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

	public List<String> getReviewProcess() {
		return reviewProcess;
	}

	public void setReviewProcess(List<String> reviewProcess) {
		this.reviewProcess = reviewProcess;
	}
}
