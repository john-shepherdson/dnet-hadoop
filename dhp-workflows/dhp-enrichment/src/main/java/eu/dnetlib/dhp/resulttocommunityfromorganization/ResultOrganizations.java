
package eu.dnetlib.dhp.resulttocommunityfromorganization;

import java.io.Serializable;
import java.util.ArrayList;

public class ResultOrganizations implements Serializable {
	private String resultId;
	private String orgId;
	private ArrayList<String> merges;

	public String getResultId() {
		return resultId;
	}

	public void setResultId(String resultId) {
		this.resultId = resultId;
	}

	public String getOrgId() {
		return orgId;
	}

	public void setOrgId(String orgId) {
		this.orgId = orgId;
	}

	public ArrayList<String> getMerges() {
		return merges;
	}

	public void setMerges(ArrayList<String> merges) {
		this.merges = merges;
	}
}
