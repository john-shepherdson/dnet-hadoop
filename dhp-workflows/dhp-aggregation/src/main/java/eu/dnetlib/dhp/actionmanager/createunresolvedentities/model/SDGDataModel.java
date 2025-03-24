
package eu.dnetlib.dhp.actionmanager.createunresolvedentities.model;

import java.io.Serializable;

public class SDGDataModel implements Serializable {

	private String doi;

	private String oaid;

	private String sdg;

	public SDGDataModel() {
	}

	public SDGDataModel(String doi, String oaid, String sdg) {
		this.doi = doi;
		this.oaid = oaid;
		this.sdg = sdg;
	}

	public static SDGDataModel newInstance(String doi, String sdg) {
		return new SDGDataModel(doi, null, sdg);
	}

	public static SDGDataModel newInstance(String doi, String oaid, String sdg) {
		return new SDGDataModel(doi, oaid, sdg);
	}

	public String getDoi() {
		return doi;
	}

	public void setDoi(String doi) {
		this.doi = doi;
	}

	public String getOaid() {
		return oaid;
	}

	public void setOaid(String oaid) {
		this.oaid = oaid;
	}

	public String getSdg() {
		return sdg;
	}

	public void setSdg(String sdg) {
		this.sdg = sdg;
	}
}
