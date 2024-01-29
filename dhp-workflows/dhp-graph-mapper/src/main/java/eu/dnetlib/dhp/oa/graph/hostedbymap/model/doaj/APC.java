
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;
import java.util.List;

public class APC implements Serializable {
	private Boolean has_apc;
	private String url;
	private List<Max> max;

	public List<Max> getMax() {
		return max;
	}

	public void setMax(List<Max> max) {
		this.max = max;
	}

	public Boolean getHas_apc() {
		return has_apc;
	}

	public void setHas_apc(Boolean has_apc) {
		this.has_apc = has_apc;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
}
