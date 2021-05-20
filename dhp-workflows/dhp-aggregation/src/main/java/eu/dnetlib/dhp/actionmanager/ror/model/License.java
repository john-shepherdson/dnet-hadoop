
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class License implements Serializable {

	@JsonProperty("attribution")
	private String attribution;

	@JsonProperty("license")
	private String license;

	private final static long serialVersionUID = -194308261058176439L;

	public String getAttribution() {
		return attribution;
	}

	public void setAttribution(final String attribution) {
		this.attribution = attribution;
	}

	public String getLicense() {
		return license;
	}

	public void setLicense(final String license) {
		this.license = license;
	}

}
