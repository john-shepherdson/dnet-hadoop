
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.io.Serializable;

/**
 * To store information about the funder funding the project related to the result. It extends eu.dnetlib.dhp.schema.dump.oaf.Funder
 * with the following parameter: -
 *
 * - private eu.dnetdlib.dhp.schema.dump.oaf.graph.Fundings funding_stream to store the fundingstream
 */
public class Funder extends eu.dnetlib.dhp.schema.dump.oaf.Funder {

	private Fundings funding_stream;

	public Fundings getFunding_stream() {
		return funding_stream;
	}

	public void setFunding_stream(Fundings funding_stream) {
		this.funding_stream = funding_stream;
	}
}
