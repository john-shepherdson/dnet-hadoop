
package eu.dnetlib.dhp.broker.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import eu.dnetlib.dhp.utils.DHPUtils;

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
		return this.subscriptionId;
	}

	public void setSubscriptionId(final String subscriptionId) {
		this.subscriptionId = subscriptionId;
	}

	public String getSubscriber() {
		return this.subscriber;
	}

	public void setSubscriber(final String subscriber) {
		this.subscriber = subscriber;
	}

	public String getTopic() {
		return this.topic;
	}

	public void setTopic(final String topic) {
		this.topic = topic;
	}

	public String getConditions() {
		return this.conditions;
	}

	public void setConditions(final String conditions) {
		this.conditions = conditions;
	}

	public Map<String, List<ConditionParams>> conditionsAsMap() {
		return conditionsAsList()
				.stream()
				.filter(mc -> !mc.getListParams().isEmpty())
				.collect(Collectors.toMap(MapCondition::getField, MapCondition::getListParams));
	}

	public List<MapCondition> conditionsAsList() {
		try {
			return DHPUtils.MAPPER
					.readValue(getConditions(), DHPUtils.MAPPER.getTypeFactory().constructCollectionType(List.class, MapCondition.class));
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}
}
