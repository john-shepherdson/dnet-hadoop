
package eu.dnetlib.dhp.broker.model;

import java.io.Serializable;

public abstract class AbstractNotification<T> implements Serializable {

	private static final long serialVersionUID = 4819926735725740170L;

	private String notificationId;

	private String subscriptionId;

	private String producerId;

	private String eventId;

	private String topic;

	private Long date;

	private String payload;

	private T map;

	public String getNotificationId() {
		return this.notificationId;
	}

	public void setNotificationId(final String notificationId) {
		this.notificationId = notificationId;
	}

	public String getSubscriptionId() {
		return this.subscriptionId;
	}

	public void setSubscriptionId(final String subscriptionId) {
		this.subscriptionId = subscriptionId;
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

	public T getMap() {
		return this.map;
	}

	public void setMap(final T map) {
		this.map = map;
	}

	public Long getDate() {
		return this.date;
	}

	public void setDate(final Long date) {
		this.date = date;
	}

}
