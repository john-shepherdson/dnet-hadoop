
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;
import java.util.List;

public class DepositPolicy implements Serializable {
	private List<String> service;
	private String url;
	private Boolean has_policy;

	public List<String> getService() {
		return service;
	}

	public void setService(List<String> service) {
		this.service = service;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Boolean getHas_policy() {
		return has_policy;
	}

	public void setHas_policy(Boolean has_policy) {
		this.has_policy = has_policy;
	}
}
