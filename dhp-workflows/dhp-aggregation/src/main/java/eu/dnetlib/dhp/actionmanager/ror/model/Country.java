
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Country implements Serializable {

	@JsonProperty("country_code")
	private String countryCode;

	@JsonProperty("country_name")
	private String countryName;

	private final static long serialVersionUID = 4357848706229493627L;

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(final String countryCode) {
		this.countryCode = countryCode;
	}

	public String getCountryName() {
		return countryName;
	}

	public void setCountryName(final String countryName) {
		this.countryName = countryName;
	}

}
