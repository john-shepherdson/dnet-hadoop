
package eu.dnetlib.dhp.actionmanager.project.utils.model;

import java.io.Serializable;

/**
 * @author miriam.baglioni
 * @Date 28/02/23
 */
public class JsonTopic implements Serializable {
	private String projectID;
	private String title;
	private String topic;

	public String getProjectID() {
		return projectID;
	}

	public void setProjectID(String projectID) {
		this.projectID = projectID;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
}
