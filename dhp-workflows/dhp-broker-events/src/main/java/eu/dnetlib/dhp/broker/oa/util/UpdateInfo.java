
package eu.dnetlib.dhp.broker.oa.util;

import java.util.function.BiConsumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.broker.objects.OaBrokerEventPayload;
import eu.dnetlib.broker.objects.OaBrokerInstance;
import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerProvenance;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.tree.support.TreeProcessor;
import eu.dnetlib.pace.util.MapDocumentUtil;

public final class UpdateInfo<T> {

	private final Topic topic;

	private final T highlightValue;

	private final OaBrokerMainEntity source;

	private final OaBrokerMainEntity target;

	private final BiConsumer<OaBrokerMainEntity, T> compileHighlight;

	private final Function<T, String> highlightToString;

	private final float trust;

	private static final Logger log = LoggerFactory.getLogger(UpdateInfo.class);

	public UpdateInfo(final Topic topic, final T highlightValue, final OaBrokerMainEntity source,
		final OaBrokerMainEntity target,
		final BiConsumer<OaBrokerMainEntity, T> compileHighlight,
		final Function<T, String> highlightToString,
		final DedupConfig dedupConfig) {
		this.topic = topic;
		this.highlightValue = highlightValue;
		this.source = source;
		this.target = target;
		this.compileHighlight = compileHighlight;
		this.highlightToString = highlightToString;
		this.trust = calculateTrust(dedupConfig, source, target);
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

	private float calculateTrust(final DedupConfig dedupConfig,
		final OaBrokerMainEntity r1,
		final OaBrokerMainEntity r2) {

		if (dedupConfig == null) {
			return BrokerConstants.MIN_TRUST;
		}

		try {
			final ObjectMapper objectMapper = new ObjectMapper();
			final MapDocument doc1 = MapDocumentUtil
				.asMapDocumentWithJPath(dedupConfig, objectMapper.writeValueAsString(r1));
			final MapDocument doc2 = MapDocumentUtil
				.asMapDocumentWithJPath(dedupConfig, objectMapper.writeValueAsString(r2));

			final double score = new TreeProcessor(dedupConfig).computeScore(doc1, doc2);
			final double threshold = dedupConfig.getWf().getThreshold();

			return TrustUtils.rescale(score, threshold);
		} catch (final Exception e) {
			log.error("Error computing score between results", e);
			return BrokerConstants.MIN_TRUST;
		}
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
		final String provRepo = getSource().getCollectedFromName();
		final String provType = getSource().getCollectedFromType();

		final String provUrl = getSource()
			.getInstances()
			.stream()
			.map(OaBrokerInstance::getUrl)
			.findFirst()
			.orElse(null);
		;

		final OaBrokerProvenance provenance = new OaBrokerProvenance(provId, provRepo, provType, provUrl);

		final OaBrokerEventPayload res = new OaBrokerEventPayload();
		res.setResult(target);
		res.setHighlight(hl);
		res.setTrust(trust);
		res.setProvenance(provenance);

		return res;
	}

}
