
package eu.dnetlib.dhp.collection;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.gson.Gson;

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

			Map<String, String> m = new HashMap<>();
			m.put(getClass().getSimpleName().toLowerCase(), new Gson().toJson(values()));
			messageSender.sendReport(m);
		}
	}
}
