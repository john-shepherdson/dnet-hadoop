
package eu.dnetlib.dhp.broker.oa.util;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.broker.objects.OpenAireEventPayload;
import eu.dnetlib.broker.objects.Provenance;
import eu.dnetlib.broker.objects.Publication;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.MapDocument;
import eu.dnetlib.pace.tree.support.TreeProcessor;
import eu.dnetlib.pace.util.MapDocumentUtil;

public final class UpdateInfo<T> {

	private final Topic topic;

	private final T highlightValue;

	private final Result source;

	private final Result target;

	private final BiConsumer<Publication, T> compileHighlight;

	private final Function<T, String> highlightToString;

	private final float trust;

	private static final Logger log = LoggerFactory.getLogger(UpdateInfo.class);

	public UpdateInfo(final Topic topic, final T highlightValue, final Result source, final Result target,
		final BiConsumer<Publication, T> compileHighlight,
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

	public Result getSource() {
		return source;
	}

	public Result getTarget() {
		return target;
	}

	private float calculateTrust(final DedupConfig dedupConfig, final Result r1, final Result r2) {
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

	public OpenAireEventPayload asBrokerPayload() {

		final Publication p = ConversionUtils.oafResultToBrokerPublication(getSource());
		compileHighlight.accept(p, getHighlightValue());

		final Publication hl = new Publication();
		compileHighlight.accept(hl, getHighlightValue());

		final String provId = getSource().getOriginalId().stream().findFirst().orElse(null);
		final String provRepo = getSource()
			.getCollectedfrom()
			.stream()
			.map(KeyValue::getValue)
			.findFirst()
			.orElse(null);
		final String provUrl = getSource()
			.getInstance()
			.stream()
			.map(Instance::getUrl)
			.flatMap(List::stream)
			.findFirst()
			.orElse(null);
		;

		final Provenance provenance = new Provenance().setId(provId).setRepositoryName(provRepo).setUrl(provUrl);

		return new OpenAireEventPayload()
			.setPublication(p)
			.setHighlight(hl)
			.setTrust(trust)
			.setProvenance(provenance);
	}

}
