
package eu.dnetlib.dhp.message;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Message implements Serializable {

	public static String CURRENT_PARAM = "current";
	public static String TOTAL_PARAM = "total";

	/**
	 *
	 */
	private static final long serialVersionUID = 401753881204524893L;

	private String workflowId;

	private Map<String, String> body;

	public Message() {
		body = new HashMap<>();
	}

	public Message(final String workflowId, final Map<String, String> body) {
		this.workflowId = workflowId;
		this.body = body;
	}

	public String getWorkflowId() {
		return workflowId;
	}

	public void setWorkflowId(final String workflowId) {
		this.workflowId = workflowId;
	}

	public Map<String, String> getBody() {
		return body;
	}

	public void setBody(final Map<String, String> body) {
		this.body = body;
	}

	@Override
	public String toString() {
		return String.format("Message [workflowId=%s, body=%s]", workflowId, body);
	}
}
