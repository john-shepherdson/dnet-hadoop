
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;

public class Waiver implements Serializable {
	private Boolean has_waiver;
	private String url;

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Boolean getHas_waiver() {
		return has_waiver;
	}

	public void setHas_waiver(Boolean has_waiver) {
		this.has_waiver = has_waiver;
	}
}
