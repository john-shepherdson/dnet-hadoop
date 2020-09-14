
package eu.dnetlib.dhp.broker.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Subscription implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 1051702214740830010L;

	private String subscriptionId;

	private String subscriber;

	private String topic;

	private String conditions;

	public String getSubscriptionId() {
		return subscriptionId;
	}

	public void setSubscriptionId(final String subscriptionId) {
		this.subscriptionId = subscriptionId;
	}

	public String getSubscriber() {
		return subscriber;
	}

	public void setSubscriber(final String subscriber) {
		this.subscriber = subscriber;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(final String topic) {
		this.topic = topic;
	}

	public String getConditions() {
		return conditions;
	}

	public void setConditions(final String conditions) {
		this.conditions = conditions;
	}

	public Map<String, List<ConditionParams>> conditionsAsMap() {
		final ObjectMapper mapper = new ObjectMapper();
		try {
			final List<MapCondition> list = mapper
				.readValue(
					getConditions(), mapper.getTypeFactory().constructCollectionType(List.class, MapCondition.class));
			return list
				.stream()
				.filter(mc -> !mc.getListParams().isEmpty())
				.collect(Collectors.toMap(MapCondition::getField, MapCondition::getListParams));
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}
}
