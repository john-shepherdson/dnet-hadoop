
package eu.dnetlib.dhp.oa.graph.hostedbymap.model;

import java.io.Serializable;

import com.opencsv.bean.CsvBindByName;

public class UnibiGoldModel implements Serializable {

	@CsvBindByName(column = "ISSN")
	private String issn;
	@CsvBindByName(column = "ISSN_L")
	private String issnL;
	@CsvBindByName(column = "TITLE")
	private String title;
	@CsvBindByName(column = "TITLE_SOURCE")
	private String titleSource;

	public String getIssn() {
		return issn;
	}

	public void setIssn(String issn) {
		this.issn = issn;
	}

	public String getIssnL() {
		return issnL;
	}

	public void setIssnL(String issnL) {
		this.issnL = issnL;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getTitleSource() {
		return titleSource;
	}

	public void setTitleSource(String titleSource) {
		this.titleSource = titleSource;
	}
}
