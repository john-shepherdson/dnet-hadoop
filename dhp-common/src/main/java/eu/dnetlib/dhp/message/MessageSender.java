
package eu.dnetlib.dhp.message;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MessageSender {

	private static final Logger log = LoggerFactory.getLogger(MessageSender.class);

	private static final int SOCKET_TIMEOUT_MS = 2000;

	private static final int CONNECTION_REQUEST_TIMEOUT_MS = 2000;

	private static final int CONNTECTION_TIMEOUT_MS = 2000;

	private final ObjectMapper objectMapper = new ObjectMapper();

	private final String dnetMessageEndpoint;

	private final String workflowId;

	private ExecutorService executorService = Executors.newCachedThreadPool();

	public MessageSender(final String dnetMessageEndpoint, final String workflowId) {
		this.workflowId = workflowId;
		this.dnetMessageEndpoint = dnetMessageEndpoint;
	}

	public void sendMessage(final Message message) {
		executorService.submit(() -> _sendMessage(message));
	}

	public void sendMessage(final Long current, final Long total) {
		sendMessage(createMessage(current, total));
	}

	private Message createMessage(final Long current, final Long total) {

		final Message m = new Message();
		m.setWorkflowId(workflowId);
		m.getBody().put(Message.CURRENT_PARAM, current.toString());
		if (total != null) {
			m.getBody().put(Message.TOTAL_PARAM, total.toString());
		}
		return m;
	}

	private void _sendMessage(final Message message) {
		try {
			final String json = objectMapper.writeValueAsString(message);

			final HttpPut req = new HttpPut(dnetMessageEndpoint);
			req.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));

			final RequestConfig requestConfig = RequestConfig
				.custom()
				.setConnectTimeout(CONNTECTION_TIMEOUT_MS)
				.setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT_MS)
				.setSocketTimeout(SOCKET_TIMEOUT_MS)
				.build();

			try (final CloseableHttpClient client = HttpClients
				.custom()
				.setDefaultRequestConfig(requestConfig)
				.build();
				final CloseableHttpResponse response = client.execute(req)) {
				log.debug("Sent Message to " + dnetMessageEndpoint);
				log.debug("MESSAGE:" + message);
			} catch (final Throwable e) {
				log.error("Error sending message to " + dnetMessageEndpoint + ", message content: " + message, e);
			}
		} catch (final JsonProcessingException e) {
			log.error("Error sending message to " + dnetMessageEndpoint + ", message content: " + message, e);
		}
	}

}
