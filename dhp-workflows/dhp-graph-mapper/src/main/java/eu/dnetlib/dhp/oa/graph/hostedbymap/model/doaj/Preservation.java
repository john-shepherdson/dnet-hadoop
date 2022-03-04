
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;
import java.util.List;

public class Preservation implements Serializable {
	private Boolean has_preservation;
	private List<String> service;
	private List<String> national_library;
	private String url;

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Boolean getHas_preservation() {
		return has_preservation;
	}

	public void setHas_preservation(Boolean has_preservation) {
		this.has_preservation = has_preservation;
	}

	public List<String> getService() {
		return service;
	}

	public void setService(List<String> service) {
		this.service = service;
	}

	public List<String> getNational_library() {
		return national_library;
	}

	public void setNational_library(List<String> national_library) {
		this.national_library = national_library;
	}
}
