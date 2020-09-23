
package eu.dnetlib.dhp.broker.oa.util.aggregators.stats;

import java.io.Serializable;

public class DatasourceStats implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -282112564184047677L;

	private String id;
	private String name;
	private String type;
	private String topic;
	private long size = 0l;

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

	public String getTopic() {
		return topic;
	}

	public void setTopic(final String topic) {
		this.topic = topic;
	}

	public long getSize() {
		return size;
	}

	public void setSize(final long size) {
		this.size = size;
	}

	public void incrementSize(final long inc) {
		this.size = this.size + inc;
	}

}
