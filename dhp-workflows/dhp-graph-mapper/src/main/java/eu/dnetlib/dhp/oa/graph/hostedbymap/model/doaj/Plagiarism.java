
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;

import javax.sql.rowset.serial.SerialArray;

public class Plagiarism implements Serializable {
	private Boolean detection;
	private String url;

	public Boolean getDetection() {
		return detection;
	}

	public void setDetection(Boolean detection) {
		this.detection = detection;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
}
