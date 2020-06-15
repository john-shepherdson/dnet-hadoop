
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.Serializable;
import java.util.List;

import eu.dnetlib.dhp.schema.dump.oaf.Projects;

public class ResultProject implements Serializable {
	private String resultId;
	private List<Projects> projectsList;

	public String getResultId() {
		return resultId;
	}

	public void setResultId(String resultId) {
		this.resultId = resultId;
	}

	public List<Projects> getProjectsList() {
		return projectsList;
	}

	public void setProjectsList(List<Projects> projectsList) {
		this.projectsList = projectsList;
	}
}
