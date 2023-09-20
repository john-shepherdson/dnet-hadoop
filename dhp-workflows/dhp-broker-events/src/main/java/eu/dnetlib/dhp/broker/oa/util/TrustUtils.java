
package eu.dnetlib.dhp.broker.oa.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.model.SparkDeduper;
import eu.dnetlib.pace.tree.support.TreeProcessor;

public class TrustUtils {

	private static final Logger log = LoggerFactory.getLogger(TrustUtils.class);

	private static DedupConfig dedupConfig;

	private static SparkDeduper deduper;

	private static final ObjectMapper mapper;

	static {
		mapper = new ObjectMapper();
		try {
			dedupConfig = DedupConfig
				.load(
					IOUtils
						.toString(
							DedupConfig.class
								.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/dedupConfig/dedupConfig.json"),
							StandardCharsets.UTF_8));

			deduper = new SparkDeduper(dedupConfig);
		} catch (final IOException e) {
			log.error("Error loading dedupConfig, e");
		}
	}

	private TrustUtils() {
	}

	protected static float calculateTrust(final OaBrokerMainEntity r1, final OaBrokerMainEntity r2) {

		if (dedupConfig == null) {
			return BrokerConstants.MIN_TRUST;
		}

		try {
			final Row doc1 = deduper.model().rowFromJson(mapper.writeValueAsString(r1));
			final Row doc2 = deduper.model().rowFromJson(mapper.writeValueAsString(r2));

			final double score = new TreeProcessor(dedupConfig).computeScore(doc1, doc2);

			final double threshold = dedupConfig.getWf().getThreshold();

			return TrustUtils.rescale(score, threshold);
		} catch (final Exception e) {
			log.error("Error computing score between results", e);
			throw new RuntimeException(e);
		}
	}

	public static float rescale(final double score, final double threshold) {
		if (score >= BrokerConstants.MAX_TRUST) {
			return BrokerConstants.MAX_TRUST;
		}

		final double val = (score - threshold) * (BrokerConstants.MAX_TRUST - BrokerConstants.MIN_TRUST)
			/ (BrokerConstants.MAX_TRUST - threshold);

		if (val < BrokerConstants.MIN_TRUST) {
			return BrokerConstants.MIN_TRUST;
		}
		if (val > BrokerConstants.MAX_TRUST) {
			return BrokerConstants.MAX_TRUST;
		}

		return (float) val;
	}
}
