
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.Serializable;

public class Country implements Serializable {

	private String countryCode;
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
