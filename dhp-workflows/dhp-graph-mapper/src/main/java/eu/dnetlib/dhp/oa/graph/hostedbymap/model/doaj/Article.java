
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;
import java.util.List;

public class Article implements Serializable {
	private String license_display_example_url;
	private List<String> license_display;
	private Boolean orcid;
	private Boolean i4oc_open_citations;

	public String getLicense_display_example_url() {
		return license_display_example_url;
	}

	public void setLicense_display_example_url(String license_display_example_url) {
		this.license_display_example_url = license_display_example_url;
	}

	public List<String> getLicense_display() {
		return license_display;
	}

	public void setLicense_display(List<String> license_display) {
		this.license_display = license_display;
	}

	public Boolean getOrcid() {
		return orcid;
	}

	public void setOrcid(Boolean orcid) {
		this.orcid = orcid;
	}

	public Boolean getI4oc_open_citations() {
		return i4oc_open_citations;
	}

	public void setI4oc_open_citations(Boolean i4oc_open_citations) {
		this.i4oc_open_citations = i4oc_open_citations;
	}
}
