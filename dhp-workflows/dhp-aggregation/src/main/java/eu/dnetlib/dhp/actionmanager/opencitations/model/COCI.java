
package eu.dnetlib.dhp.actionmanager.opencitations.model;

import java.io.Serializable;

import com.opencsv.bean.CsvBindByPosition;

public class COCI implements Serializable {
	private String oci;

	private String citing;
	private String citing_pid;

	private String cited;
	private String cited_pid;

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

	public String getCiting_pid() {
		return citing_pid;
	}

	public void setCiting_pid(String citing_pid) {
		this.citing_pid = citing_pid;
	}

	public String getCited_pid() {
		return cited_pid;
	}

	public void setCited_pid(String cited_pid) {
		this.cited_pid = cited_pid;
	}
}
