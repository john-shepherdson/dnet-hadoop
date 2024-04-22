
package eu.dnetlib.dhp.actionmanager.transformativeagreement.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author miriam.baglioni
 * @Date 18/12/23
 */
@JsonIgnoreProperties(ignoreUnknown = true)

public class TransformativeAgreementModel implements Serializable {
	private String institution;
	private String doi;
	private String agreement;
	private String country;

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getInstitution() {
		return institution;
	}

	public void setInstitution(String institution) {
		this.institution = institution;
	}

	public String getDoi() {
		return doi;
	}

	public void setDoi(String doi) {
		this.doi = doi;
	}

	public String getAgreement() {
		return agreement;
	}

	public void setAgreement(String agreement) {
		this.agreement = agreement;
	}
}
