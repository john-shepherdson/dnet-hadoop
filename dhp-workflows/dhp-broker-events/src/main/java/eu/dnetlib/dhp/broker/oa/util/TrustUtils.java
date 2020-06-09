package eu.dnetlib.dhp.broker.oa.util;

public class TrustUtils {

	public static float rescale(final double score, final double threshold) {
		if (score >= BrokerConstants.MAX_TRUST) { return BrokerConstants.MAX_TRUST; }

		final double val = (score - threshold) * (BrokerConstants.MAX_TRUST - BrokerConstants.MIN_TRUST) / (BrokerConstants.MAX_TRUST - threshold);

		if (val < BrokerConstants.MIN_TRUST) { return BrokerConstants.MIN_TRUST; }
		if (val > BrokerConstants.MAX_TRUST) { return BrokerConstants.MAX_TRUST; }

		return (float) val;
	}
}
