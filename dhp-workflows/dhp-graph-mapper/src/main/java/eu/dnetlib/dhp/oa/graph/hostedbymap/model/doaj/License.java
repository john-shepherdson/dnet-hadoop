
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class License implements Serializable {
	private Boolean nc;
	private Boolean nd;
	private Boolean by;
	private String type;
	private Boolean sa;
	private String url;

	public Boolean getnC() {
		return nc;
	}

	@JsonProperty("NC")
	public void setnC(Boolean NC) {
		this.nc = NC;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Boolean getNd() {
		return nd;
	}

	@JsonProperty("ND")
	public void setNd(Boolean nd) {
		this.nd = nd;
	}

	public Boolean getBy() {
		return by;
	}

	@JsonProperty("BY")
	public void setBy(Boolean by) {
		this.by = by;
	}

	public Boolean getSa() {
		return sa;
	}

	@JsonProperty("SA")
	public void setSa(Boolean sa) {
		this.sa = sa;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
}
