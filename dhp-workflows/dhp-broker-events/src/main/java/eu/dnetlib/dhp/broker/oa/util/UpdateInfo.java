
package eu.dnetlib.dhp.broker.oa.util;

import java.util.function.BiConsumer;
import java.util.function.Function;

import eu.dnetlib.broker.objects.OaBrokerEventPayload;
import eu.dnetlib.broker.objects.OaBrokerInstance;
import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerProvenance;
import eu.dnetlib.broker.objects.OaBrokerRelatedDatasource;
import eu.dnetlib.dhp.broker.model.Topic;

public final class UpdateInfo<T> {

	private final Topic topic;

	private final T highlightValue;

	private final OaBrokerMainEntity source;

	private final OaBrokerMainEntity target;

	private final OaBrokerRelatedDatasource targetDs;

	private final BiConsumer<OaBrokerMainEntity, T> compileHighlight;

	private final Function<T, String> highlightToString;

	private final float trust;

	public UpdateInfo(final Topic topic, final T highlightValue, final OaBrokerMainEntity source,
		final OaBrokerMainEntity target,
		final OaBrokerRelatedDatasource targetDs,
		final BiConsumer<OaBrokerMainEntity, T> compileHighlight,
		final Function<T, String> highlightToString) {
		this.topic = topic;
		this.highlightValue = highlightValue;
		this.source = source;
		this.target = target;
		this.targetDs = targetDs;
		this.compileHighlight = compileHighlight;
		this.highlightToString = highlightToString;
		this.trust = TrustUtils.calculateTrust(source, target);
	}

	public T getHighlightValue() {
		return highlightValue;
	}

	public OaBrokerMainEntity getSource() {
		return source;
	}

	public OaBrokerMainEntity getTarget() {
		return target;
	}

	public OaBrokerRelatedDatasource getTargetDs() {
		return targetDs;
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

	public String getHighlightValueAsString() {
		return highlightToString.apply(getHighlightValue());
	}

	public OaBrokerEventPayload asBrokerPayload() {

		compileHighlight.accept(target, getHighlightValue());

		final OaBrokerMainEntity hl = new OaBrokerMainEntity();
		compileHighlight.accept(hl, getHighlightValue());

		final String provId = getSource().getOpenaireId();
		final String provRepo = getSource()
			.getDatasources()
			.stream()
			.filter(ds -> ds.getRelType().equals(BrokerConstants.COLLECTED_FROM_REL))
			.map(ds -> ds.getName())
			.findFirst()
			.orElse("");
		final String provType = getSource()
			.getDatasources()
			.stream()
			.filter(ds -> ds.getRelType().equals(BrokerConstants.COLLECTED_FROM_REL))
			.map(ds -> ds.getType())
			.findFirst()
			.orElse("");

		final String provUrl = getSource()
			.getInstances()
			.stream()
			.map(OaBrokerInstance::getUrl)
			.findFirst()
			.orElse(null);

		final OaBrokerProvenance provenance = new OaBrokerProvenance(provId, provRepo, provType, provUrl);

		final OaBrokerEventPayload res = new OaBrokerEventPayload();
		res.setResult(target);
		res.setHighlight(hl);
		res.setTrust(trust);
		res.setProvenance(provenance);

		return res;
	}

}
