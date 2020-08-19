
package eu.dnetlib.dhp.schema.dump.oaf.community;

import java.io.Serializable;

/**
 * To store information about the funder funding the project related to the result. It has the following parameters: -
 * shortName of type String to store the funder short name (e.c. AKA). - name of type String to store the funder name
 * (e.c. Akademy of Finland) - fundingStream of type String to store the funding stream - jurisdiction of type String to
 * store the jurisdiction of the funder
 */
public class Funder extends eu.dnetlib.dhp.schema.dump.oaf.Funder {
//	private String shortName;
//
//	private String name;

	private String fundingStream;

//	private String jurisdiction;

//	public String getJurisdiction() {
//		return jurisdiction;
//	}
//
//	public void setJurisdiction(String jurisdiction) {
//		this.jurisdiction = jurisdiction;
//	}
//
//	public String getShortName() {
//		return shortName;
//	}
//
//	public void setShortName(String shortName) {
//		this.shortName = shortName;
//	}
//
//	public String getName() {
//		return name;
//	}
//
//	public void setName(String name) {
//		this.name = name;
//	}

	public String getFundingStream() {
		return fundingStream;
	}

	public void setFundingStream(String fundingStream) {
		this.fundingStream = fundingStream;
	}
}
