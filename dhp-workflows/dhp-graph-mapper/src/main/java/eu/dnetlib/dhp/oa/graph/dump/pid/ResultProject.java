
package eu.dnetlib.dhp.oa.graph.dump.pid;

import java.io.Serializable;
import java.util.List;

public class ResultProject implements Serializable {
	private String resultId;
	private String code;
	private List<String> fundings;

	public String getResultId() {
		return resultId;
	}

	public void setResultId(String resultId) {
		this.resultId = resultId;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public List<String> getFundings() {
		return fundings;
	}

	public void setFundings(List<String> fundings) {
		this.fundings = fundings;
	}
}
