
package eu.dnetlib.dhp.oa.graph.dump.funderresults;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.schema.dump.oaf.Result;

public class FunderResults implements Serializable {
	private List<Result> results;

	public List<Result> getResults() {
		return results;
	}

	public void setResults(List<Result> results) {
		this.results = results;
	}
}
