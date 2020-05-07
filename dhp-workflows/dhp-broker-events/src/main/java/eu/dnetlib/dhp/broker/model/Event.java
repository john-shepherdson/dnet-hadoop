package eu.dnetlib.dhp.broker.model;

import java.util.Map;

public class Event {

	private String eventId;

	private String producerId;

	private String topic;

	private String payload;

	private Long creationDate;

	private Long expiryDate;

	private boolean instantMessage;

	private Map<String, Object> map;

	public Event() {}

	public Event(final String producerId, final String eventId, final String topic, final String payload, final Long creationDate, final Long expiryDate,
		final boolean instantMessage,
		final Map<String, Object> map) {
		this.producerId = producerId;
		this.eventId = eventId;
		this.topic = topic;
		this.payload = payload;
		this.creationDate = creationDate;
		this.expiryDate = expiryDate;
		this.instantMessage = instantMessage;
		this.map = map;
	}

	public String getProducerId() {
		return this.producerId;
	}

	public void setProducerId(final String producerId) {
		this.producerId = producerId;
	}

	public String getEventId() {
		return this.eventId;
	}

	public void setEventId(final String eventId) {
		this.eventId = eventId;
	}

	public String getTopic() {
		return this.topic;
	}

	public void setTopic(final String topic) {
		this.topic = topic;
	}

	public String getPayload() {
		return this.payload;
	}

	public void setPayload(final String payload) {
		this.payload = payload;
	}

	public Long getCreationDate() {
		return this.creationDate;
	}

	public void setCreationDate(final Long creationDate) {
		this.creationDate = creationDate;
	}

	public Long getExpiryDate() {
		return this.expiryDate;
	}

	public void setExpiryDate(final Long expiryDate) {
		this.expiryDate = expiryDate;
	}

	public boolean isInstantMessage() {
		return this.instantMessage;
	}

	public void setInstantMessage(final boolean instantMessage) {
		this.instantMessage = instantMessage;
	}

	public Map<String, Object> getMap() {
		return this.map;
	}

	public void setMap(final Map<String, Object> map) {
		this.map = map;
	}
}
