
package eu.dnetlib.dhp.message;

import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

public enum MessageType {

	ONGOING, REPORT;

	public MessageType from(String value) {
		return Optional
			.ofNullable(value)
			.map(StringUtils::upperCase)
			.map(MessageType::valueOf)
			.orElseThrow(() -> new IllegalArgumentException("unknown message type: " + value));
	}

}
