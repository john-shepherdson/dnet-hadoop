
package eu.dnetlib.dhp.oa.graph.dump.funderresults;

import java.io.Serializable;

import eu.dnetlib.dhp.schema.dump.oaf.Result;

public class FunderResults extends Result implements Serializable {
	private String funder_id;

	public String getFunder_id() {
		return funder_id;
	}

	public void setFunder_id(String funder_id) {
		this.funder_id = funder_id;
	}
}
