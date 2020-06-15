
package eu.dnetlib.dhp.schema.dump.oaf;

import java.io.Serializable;

public class Funder implements Serializable {
	private String shortName;

	private String name;

	private String fundingStream;

	private String jurisdiction;

	public String getJurisdiction() {
		return jurisdiction;
	}

	public void setJurisdiction(String jurisdiction) {
		this.jurisdiction = jurisdiction;
	}

	public String getShortName() {
		return shortName;
	}

	public void setShortName(String shortName) {
		this.shortName = shortName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFundingStream() {
		return fundingStream;
	}

	public void setFundingStream(String fundingStream) {
		this.fundingStream = fundingStream;
	}
}
