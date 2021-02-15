
package eu.dnetlib.dhp.collection;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.message.MessageSender;

public class CollectorPluginReport extends LinkedHashMap<String, String> implements Closeable {

	private static final Logger log = LoggerFactory.getLogger(CollectorPluginReport.class);

	private MessageSender messageSender;

	public CollectorPluginReport() {
	}

	public CollectorPluginReport(MessageSender messageSender) throws IOException {
		this.messageSender = messageSender;
	}

	public void ongoing(Long current, Long total) {
		messageSender.sendMessage(current, total);
	}

	@Override
	public void close() throws IOException {
		if (Objects.nonNull(messageSender)) {
			log.info("closing report: ");
			this.forEach((k, v) -> log.info("{} - {}", k, v));
			messageSender.sendReport(this);
		}
	}
}
