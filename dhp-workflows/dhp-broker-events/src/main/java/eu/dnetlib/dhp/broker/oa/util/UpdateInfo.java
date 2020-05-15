
package eu.dnetlib.dhp.broker.oa.util;

import java.util.function.BiConsumer;
import java.util.function.Function;

import eu.dnetlib.broker.objects.OpenAireEventPayload;
import eu.dnetlib.broker.objects.Publication;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.oaf.Result;

public final class UpdateInfo<T> {

	private final Topic topic;

	private final T highlightValue;

	private final Result source;

	private final Result target;

	private final BiConsumer<Publication, T> compileHighlight;

	private final Function<T, String> highlightToString;

	private final float trust;

	public UpdateInfo(final Topic topic, final T highlightValue, final Result source, final Result target,
		final BiConsumer<Publication, T> compileHighlight,
		final Function<T, String> highlightToString) {
		this.topic = topic;
		this.highlightValue = highlightValue;
		this.source = source;
		this.target = target;
		this.compileHighlight = compileHighlight;
		this.highlightToString = highlightToString;
		this.trust = calculateTrust(source, target);
	}

	public T getHighlightValue() {
		return highlightValue;
	}

	public Result getSource() {
		return source;
	}

	public Result getTarget() {
		return target;
	}

	private float calculateTrust(final Result source, final Result target) {
		// TODO
		return 0.9f;
	}

	protected Topic getTopic() {
		return topic;
	}

	public String getTopicPath() {
		return topic.getPath();
	}

	public float getTrust() {
		return trust;
	}

	public void compileHighlight(final OpenAireEventPayload payload) {
		compileHighlight.accept(payload.getHighlight(), getHighlightValue());
	}

	public String getHighlightValueAsString() {
		return highlightToString.apply(getHighlightValue());
	}

}
