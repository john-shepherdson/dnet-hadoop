
package eu.dnetlib.dhp.oa.graph.dump.funderresults;

import java.io.Serializable;

import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;

public class FunderResults extends CommunityResult implements Serializable {
	private String funder_id;

	public String getFunder_id() {
		return funder_id;
	}

	public void setFunder_id(String funder_id) {
		this.funder_id = funder_id;
	}
}
