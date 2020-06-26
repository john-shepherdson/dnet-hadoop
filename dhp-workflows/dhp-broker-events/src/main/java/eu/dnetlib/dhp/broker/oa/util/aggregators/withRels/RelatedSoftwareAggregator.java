
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;
import scala.Tuple2;

public class RelatedSoftwareAggregator
	extends Aggregator<Tuple2<OaBrokerMainEntity, RelatedSoftware>, OaBrokerMainEntity, OaBrokerMainEntity> {

	/**
	 *
	 */
	private static final long serialVersionUID = -8987959389106443702L;

	@Override
	public OaBrokerMainEntity zero() {
		return new OaBrokerMainEntity();
	}

	@Override
	public OaBrokerMainEntity finish(final OaBrokerMainEntity g) {
		return g;
	}

	@Override
	public OaBrokerMainEntity reduce(final OaBrokerMainEntity g, final Tuple2<OaBrokerMainEntity, RelatedSoftware> t) {
		final OaBrokerMainEntity res = StringUtils.isNotBlank(g.getOpenaireId()) ? g : t._1;
		if (t._2 != null && res.getSoftwares().size() < BrokerConstants.MAX_NUMBER_OF_RELS) {
			res.getSoftwares().add(t._2.getRelSoftware());
		}
		return res;

	}

	@Override
	public OaBrokerMainEntity merge(final OaBrokerMainEntity g1, final OaBrokerMainEntity g2) {
		if (StringUtils.isNotBlank(g1.getOpenaireId())) {
			final int availables = BrokerConstants.MAX_NUMBER_OF_RELS - g1.getSoftwares().size();
			if (availables > 0) {
				if (g2.getSoftwares().size() <= availables) {
					g1.getSoftwares().addAll(g2.getSoftwares());
				} else {
					g1.getSoftwares().addAll(g2.getSoftwares().subList(0, availables));
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
