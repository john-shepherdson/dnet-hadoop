
package eu.dnetlib.dhp.actionmanager.opencitations.model;

import java.io.Serializable;

import com.opencsv.bean.CsvBindByPosition;

public class COCI implements Serializable {
	private String oci;

	private String citing;

	private String cited;

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

}
