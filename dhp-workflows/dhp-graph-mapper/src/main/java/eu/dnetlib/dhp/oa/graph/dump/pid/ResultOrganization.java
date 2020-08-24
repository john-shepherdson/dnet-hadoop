
package eu.dnetlib.dhp.oa.graph.dump.pid;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class ResultOrganization implements Serializable {
	private String resultId;
	private List<StructuredProperty> orgPids;

	public String getResultId() {
		return resultId;
	}

	public void setResultId(String resultId) {
		this.resultId = resultId;
	}

	public List<StructuredProperty> getOrgPid() {
		return orgPids;
	}

	public void setOrgPid(List<StructuredProperty> pid) {
		this.orgPids = pid;
	}
}
