
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

/**
 * To store information about the funder funding the project related to the result. It has the following parameters:
 * - private String shortName to store the short name of the funder (e.g. AKA)
 * - private String name to store information about the name of the funder (e.g. Akademy of Finland)
 * - private Fundings funding_stream to store the fundingstream
 * - private String jurisdiction to store information about the jurisdiction of the funder
 */
public class Funder implements Serializable {

	private String shortName;

	private String name;

	private Fundings funding_stream;

	private String jurisdiction;

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

	public String getJurisdiction() {
		return jurisdiction;
	}

	public void setJurisdiction(String jurisdiction) {
		this.jurisdiction = jurisdiction;
	}

	public Fundings getFunding_stream() {
		return funding_stream;
	}

	public void setFunding_stream(Fundings funding_stream) {
		this.funding_stream = funding_stream;
	}
}
