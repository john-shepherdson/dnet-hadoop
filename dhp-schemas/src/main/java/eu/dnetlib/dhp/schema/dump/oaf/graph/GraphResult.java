
package eu.dnetlib.dhp.schema.dump.oaf.graph;

import java.util.List;

import eu.dnetlib.dhp.schema.dump.oaf.Instance;
import eu.dnetlib.dhp.schema.dump.oaf.Result;

/**
 * It extends the eu.dnetlib.dhp.schema.dump.oaf.Result with - instance of type
 * List<eu.dnetlib.dhp.schema.dump.oaf.Instance> to store all the instances associated to the result. It corresponds to
 * the same parameter in the result represented in the internal model
 */
public class GraphResult extends Result {
	private List<Instance> instance;

	public List<Instance> getInstance() {
		return instance;
	}

	public void setInstance(List<Instance> instance) {
		this.instance = instance;
	}
}
