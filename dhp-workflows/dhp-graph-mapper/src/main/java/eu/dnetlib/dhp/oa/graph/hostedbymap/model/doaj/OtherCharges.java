
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;

public class OtherCharges implements Serializable {
	private Boolean has_other_charges;
	private String url;

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Boolean getHas_other_charges() {
		return has_other_charges;
	}

	public void setHas_other_charges(Boolean has_other_charges) {
		this.has_other_charges = has_other_charges;
	}
}
