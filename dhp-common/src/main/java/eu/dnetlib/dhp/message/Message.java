
package eu.dnetlib.dhp.message;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Message implements Serializable {

	private static final long serialVersionUID = 401753881204524893L;

	public static String CURRENT_PARAM = "current";
	public static String TOTAL_PARAM = "total";

	private MessageType messageType;

	private String workflowId;

	private Map<String, String> body;

	public Message() {
	}

	public Message(final MessageType messageType, final String workflowId) {
		this(messageType, workflowId, new LinkedHashMap<>());
	}

	public Message(final MessageType messageType, final String workflowId, final Map<String, String> body) {
		this.messageType = messageType;
		this.workflowId = workflowId;
		this.body = body;
	}

	public MessageType getMessageType() {
		return messageType;
	}

	public void setMessageType(MessageType messageType) {
		this.messageType = messageType;
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
		return String.format("Message [type=%s, workflowId=%s, body=%s]", messageType, workflowId, body);
	}

}
