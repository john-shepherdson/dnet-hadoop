
package eu.dnetlib.dhp.broker.model;

import java.io.Serializable;

public class Notification implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -1770420972526995727L;

	private String notificationId;

	private String subscriptionId;

	private String producerId;

	private String eventId;

	private String topic;

	private Long date;

	private String payload;

	private MappedFields map;

	public String getNotificationId() {
		return notificationId;
	}

	public void setNotificationId(final String notificationId) {
		this.notificationId = notificationId;
	}

	public String getSubscriptionId() {
		return subscriptionId;
	}

	public void setSubscriptionId(final String subscriptionId) {
		this.subscriptionId = subscriptionId;
	}

	public String getProducerId() {
		return producerId;
	}

	public void setProducerId(final String producerId) {
		this.producerId = producerId;
	}

	public String getEventId() {
		return eventId;
	}

	public void setEventId(final String eventId) {
		this.eventId = eventId;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(final String topic) {
		this.topic = topic;
	}

	public String getPayload() {
		return payload;
	}

	public void setPayload(final String payload) {
		this.payload = payload;
	}

	public MappedFields getMap() {
		return map;
	}

	public void setMap(final MappedFields map) {
		this.map = map;
	}

	public Long getDate() {
		return date;
	}

	public void setDate(final Long date) {
		this.date = date;
	}

}
