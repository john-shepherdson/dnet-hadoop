package eu.dnetlib.dhp.broker.oa.util;

import eu.dnetlib.broker.objects.OpenAireEventPayload;

public abstract class UpdateInfo<T> {

	private final String topic;

	private final T highlightValue;

	private final float trust;

	protected UpdateInfo(final String topic, final T highlightValue, final float trust) {
		this.topic = topic;
		this.highlightValue = highlightValue;
		this.trust = trust;
	}

	public T getHighlightValue() {
		return highlightValue;
	}

	public float getTrust() {
		return trust;
	}

	public String getTopic() {
		return topic;
	}

	abstract public void compileHighlight(OpenAireEventPayload payload);

	abstract public String getHighlightValueAsString();

}
