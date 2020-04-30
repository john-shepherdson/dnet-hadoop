
package eu.dnetlib.dhp.resulttoorganizationfrominstrepo;

import java.io.Serializable;
import java.util.ArrayList;

public class ResultOrganizationSet implements Serializable {
	private String resultId;
	private ArrayList<String> organizationSet;

	public String getResultId() {
		return resultId;
	}

	public void setResultId(String resultId) {
		this.resultId = resultId;
	}

	public ArrayList<String> getOrganizationSet() {
		return organizationSet;
	}

	public void setOrganizationSet(ArrayList<String> organizationSet) {
		this.organizationSet = organizationSet;
	}
}
