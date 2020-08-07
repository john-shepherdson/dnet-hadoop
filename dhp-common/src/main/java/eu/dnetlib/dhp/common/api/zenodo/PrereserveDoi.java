
package eu.dnetlib.dhp.common.api.zenodo;

import java.io.Serializable;

public class PrereserveDoi implements Serializable {
	private String doi;
	private String recid;

	public String getDoi() {
		return doi;
	}

	public void setDoi(String doi) {
		this.doi = doi;
	}

	public String getRecid() {
		return recid;
	}

	public void setRecid(String recid) {
		this.recid = recid;
	}
}
