
package eu.dnetlib.message;

import java.io.Serializable;
import java.util.Map;

public class Message implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 401753881204524893L;

	private String workflowId;

	private String jobName;

	private MessageType type;

	private Map<String, String> body;

	public Message() {
	}

	public Message(final String workflowId, final String jobName, final MessageType type,
		final Map<String, String> body) {
		this.workflowId = workflowId;
		this.jobName = jobName;
		this.type = type;
		this.body = body;
	}

	public String getWorkflowId() {
		return workflowId;
	}

	public void setWorkflowId(final String workflowId) {
		this.workflowId = workflowId;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(final String jobName) {
		this.jobName = jobName;
	}

	public MessageType getType() {
		return type;
	}

	public void setType(final MessageType type) {
		this.type = type;
	}

	public Map<String, String> getBody() {
		return body;
	}

	public void setBody(final Map<String, String> body) {
		this.body = body;
	}

	@Override
	public String toString() {
		return String.format("Message [workflowId=%s, jobName=%s, type=%s, body=%s]", workflowId, jobName, type, body);
	}
}
