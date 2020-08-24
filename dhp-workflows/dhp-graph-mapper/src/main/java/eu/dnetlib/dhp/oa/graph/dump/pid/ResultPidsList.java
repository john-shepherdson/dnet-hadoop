
package eu.dnetlib.dhp.oa.graph.dump.pid;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.schema.dump.oaf.KeyValue;

/**
 * Needed to create relations between pids in the result. The list of resultAllowedPids will produce relation of type
 * source hasOtherMaterialization target (and vice-versa) where source will be identified by one of the pids in the list
 * and target by another. A couple of relation between every two nodes. The list of authorAllowedPids will produce
 * relation of type source hasAuthor target and target isAuthorOf source for every couple of nodes in result and author.
 */
public class ResultPidsList implements Serializable {
	private String resultId;
	private List<KeyValue> resultAllowedPids;
	private List<List<KeyValue>> authorAllowedPids;

	public String getResultId() {
		return resultId;
	}

	public void setResultId(String resultId) {
		this.resultId = resultId;
	}

	public List<KeyValue> getResultAllowedPids() {
		return resultAllowedPids;
	}

	public void setResultAllowedPids(List<KeyValue> resultAllowedPids) {
		this.resultAllowedPids = resultAllowedPids;
	}

	public List<List<KeyValue>> getAuthorAllowedPids() {
		return authorAllowedPids;
	}

	public void setAuthorAllowedPids(List<List<KeyValue>> authorAllowedPids) {
		this.authorAllowedPids = authorAllowedPids;
	}
}
