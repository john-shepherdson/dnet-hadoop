
package eu.dnetlib.message;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.SerializableEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageSender {

	private static final Logger log = LoggerFactory.getLogger(MessageSender.class);

	private final String dnetMessageEndpoint;

	public MessageSender(final String dnetMessageEndpoint) {
		this.dnetMessageEndpoint = dnetMessageEndpoint;
	}

	public void sendMessage(final Message message) {
		final HttpPut req = new HttpPut(dnetMessageEndpoint);
		req.setEntity(new SerializableEntity(message));

		try (final CloseableHttpClient client = HttpClients.createDefault();
			final CloseableHttpResponse response = client.execute(req)) {
			log.debug("Sent Message to " + dnetMessageEndpoint);
			log.debug("MESSAGE:" + message);
		} catch (final Throwable e) {
			log.error("Error sending message to " + dnetMessageEndpoint + ", message content: " + message, e);
		}
	}

}
