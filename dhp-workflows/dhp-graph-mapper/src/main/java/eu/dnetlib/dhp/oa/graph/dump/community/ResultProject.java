
package eu.dnetlib.dhp.oa.graph.dump.community;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.schema.dump.oaf.community.Project;

public class ResultProject implements Serializable {
	private String resultId;
	private List<Project> projectsList;

	public String getResultId() {
		return resultId;
	}

	public void setResultId(String resultId) {
		this.resultId = resultId;
	}

	public List<Project> getProjectsList() {
		return projectsList;
	}

	public void setProjectsList(List<Project> projectsList) {
		this.projectsList = projectsList;
	}
}
