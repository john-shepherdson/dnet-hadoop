
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;
import scala.Tuple2;

public class RelatedDatasetAggregator
	extends Aggregator<Tuple2<OaBrokerMainEntity, RelatedDataset>, OaBrokerMainEntity, OaBrokerMainEntity> {

	/**
	 *
	 */
	private static final long serialVersionUID = 6969761680131482557L;

	@Override
	public OaBrokerMainEntity zero() {
		return new OaBrokerMainEntity();
	}

	@Override
	public OaBrokerMainEntity finish(final OaBrokerMainEntity g) {
		return g;
	}

	@Override
	public OaBrokerMainEntity reduce(final OaBrokerMainEntity g, final Tuple2<OaBrokerMainEntity, RelatedDataset> t) {
		final OaBrokerMainEntity res = StringUtils.isNotBlank(g.getOpenaireId()) ? g : t._1;
		if (t._2 != null && res.getDatasets().size() < BrokerConstants.MAX_NUMBER_OF_RELS) {
			res.getDatasets().add(t._2.getRelDataset());
		}
		return res;

	}

	@Override
	public OaBrokerMainEntity merge(final OaBrokerMainEntity g1, final OaBrokerMainEntity g2) {
		if (StringUtils.isNotBlank(g1.getOpenaireId())) {
			final int availables = BrokerConstants.MAX_NUMBER_OF_RELS - g1.getDatasets().size();
			if (availables > 0) {
				if (g2.getDatasets().size() <= availables) {
					g1.getDatasets().addAll(g2.getDatasets());
				} else {
					g1.getDatasets().addAll(g2.getDatasets().subList(0, availables));
				}
			}
			return g1;
		} else {
			return g2;
		}
	}

	@Override
	public Encoder<OaBrokerMainEntity> bufferEncoder() {
		return Encoders.bean(OaBrokerMainEntity.class);
	}

	@Override
	public Encoder<OaBrokerMainEntity> outputEncoder() {
		return Encoders.bean(OaBrokerMainEntity.class);
	}

}
