
package eu.dnetlib.dhp.actionmanager.opencitations.model;

import java.io.Serializable;

import com.opencsv.bean.CsvBindByPosition;

public class COCI implements Serializable {
	@CsvBindByPosition(position = 0)
//    @CsvBindByName(column = "doi")
	private String oci;

	@CsvBindByPosition(position = 1)
//    @CsvBindByName(column = "level1")
	private String citing;

	@CsvBindByPosition(position = 2)
//    @CsvBindByName(column = "level2")
	private String cited;

	@CsvBindByPosition(position = 3)
//    @CsvBindByName(column = "level3")
	private String creation;

	@CsvBindByPosition(position = 4)
	private String timespan;

	@CsvBindByPosition(position = 5)
	private String journal_sc;

	@CsvBindByPosition(position = 6)
	private String author_sc;

	public String getOci() {
		return oci;
	}

	public void setOci(String oci) {
		this.oci = oci;
	}

	public String getCiting() {
		return citing;
	}

	public void setCiting(String citing) {
		this.citing = citing;
	}

	public String getCited() {
		return cited;
	}

	public void setCited(String cited) {
		this.cited = cited;
	}

	public String getCreation() {
		return creation;
	}

	public void setCreation(String creation) {
		this.creation = creation;
	}

	public String getTimespan() {
		return timespan;
	}

	public void setTimespan(String timespan) {
		this.timespan = timespan;
	}

	public String getJournal_sc() {
		return journal_sc;
	}

	public void setJournal_sc(String journal_sc) {
		this.journal_sc = journal_sc;
	}

	public String getAuthor_sc() {
		return author_sc;
	}

	public void setAuthor_sc(String author_sc) {
		this.author_sc = author_sc;
	}
}
