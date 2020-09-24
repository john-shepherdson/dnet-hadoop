
package eu.dnetlib.dhp.actionmanager.project;

import java.io.Serializable;

public class ProjectSubset implements Serializable {

	private String code;
	private String topiccode;
	private String topicdescription;

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getTopiccode() {
		return topiccode;
	}

	public void setTopiccode(String topiccode) {
		this.topiccode = topiccode;
	}

	public String getTopicdescription() {
		return topicdescription;
	}

	public void setTopicdescription(String topicdescription) {
		this.topicdescription = topicdescription;
	}
}
