
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;
import scala.Tuple2;

public class RelatedPublicationAggregator
	extends Aggregator<Tuple2<OaBrokerMainEntity, RelatedPublication>, OaBrokerMainEntity, OaBrokerMainEntity> {

	/**
	 *
	 */
	private static final long serialVersionUID = 4656934981558135919L;

	@Override
	public OaBrokerMainEntity zero() {
		return new OaBrokerMainEntity();
	}

	@Override
	public OaBrokerMainEntity finish(final OaBrokerMainEntity g) {
		return g;
	}

	@Override
	public OaBrokerMainEntity reduce(final OaBrokerMainEntity g,
		final Tuple2<OaBrokerMainEntity, RelatedPublication> t) {
		final OaBrokerMainEntity res = StringUtils.isNotBlank(g.getOpenaireId()) ? g : t._1;
		if (t._2 != null && res.getPublications().size() < BrokerConstants.MAX_NUMBER_OF_RELS) {
			res.getPublications().add(t._2.getRelPublication());
		}
		return res;

	}

	@Override
	public OaBrokerMainEntity merge(final OaBrokerMainEntity g1, final OaBrokerMainEntity g2) {
		if (StringUtils.isNotBlank(g1.getOpenaireId())) {
			final int availables = BrokerConstants.MAX_NUMBER_OF_RELS - g1.getPublications().size();
			if (availables > 0) {
				if (g2.getPublications().size() <= availables) {
					g1.getPublications().addAll(g2.getPublications());
				} else {
					g1.getPublications().addAll(g2.getPublications().subList(0, availables));
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
