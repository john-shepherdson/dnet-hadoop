
package eu.dnetlib.dhp.broker.oa.util.aggregators.stats;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DatasourceStats implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -282112564184047677L;

	private String id;
	private String name;
	private String type;
	private Map<String, Long> topics = new HashMap<>();

	public String getId() {
		return id;
	}

	public void setId(final String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(final String type) {
		this.type = type;
	}

	public Map<String, Long> getTopics() {
		return topics;
	}

	public void setTopics(final Map<String, Long> topics) {
		this.topics = topics;
	}

	public void incrementTopic(final String topic, final long inc) {
		if (topics.containsKey(topic)) {
			topics.put(topic, topics.get(topic) + inc);
		} else {
			topics.put(topic, inc);
		}

	}

}
