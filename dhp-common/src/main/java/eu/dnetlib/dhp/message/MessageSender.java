
package eu.dnetlib.dhp.message;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.SerializableEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageSender {

	private static final Logger log = LoggerFactory.getLogger(MessageSender.class);

	private static final int SOCKET_TIMEOUT_MS = 2000;

	private static final int CONNECTION_REQUEST_TIMEOUT_MS = 2000;

	private static final int CONNTECTION_TIMEOUT_MS = 2000;

	private final String dnetMessageEndpoint;

	public MessageSender(final String dnetMessageEndpoint) {
		this.dnetMessageEndpoint = dnetMessageEndpoint;
	}

	public void sendMessage(final Message message) {
		new Thread(() -> _sendMessage(message)).start();
	}

	private void _sendMessage(final Message message) {
		final HttpPut req = new HttpPut(dnetMessageEndpoint);
		req.setEntity(new SerializableEntity(message));

		final RequestConfig requestConfig = RequestConfig
			.custom()
			.setConnectTimeout(CONNTECTION_TIMEOUT_MS)
			.setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT_MS)
			.setSocketTimeout(SOCKET_TIMEOUT_MS)
			.build();
		;

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
	}

}
