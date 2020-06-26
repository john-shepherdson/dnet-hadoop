
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;
import scala.Tuple2;

public class RelatedProjectAggregator
	extends Aggregator<Tuple2<OaBrokerMainEntity, RelatedProject>, OaBrokerMainEntity, OaBrokerMainEntity> {

	/**
	 *
	 */
	private static final long serialVersionUID = 8559808519152275763L;

	@Override
	public OaBrokerMainEntity zero() {
		return new OaBrokerMainEntity();
	}

	@Override
	public OaBrokerMainEntity finish(final OaBrokerMainEntity g) {
		return g;
	}

	@Override
	public OaBrokerMainEntity reduce(final OaBrokerMainEntity g, final Tuple2<OaBrokerMainEntity, RelatedProject> t) {
		final OaBrokerMainEntity res = StringUtils.isNotBlank(g.getOpenaireId()) ? g : t._1;
		if (t._2 != null && res.getProjects().size() < BrokerConstants.MAX_NUMBER_OF_RELS) {
			res.getProjects().add(t._2.getRelProject());
		}
		return res;

	}

	@Override
	public OaBrokerMainEntity merge(final OaBrokerMainEntity g1, final OaBrokerMainEntity g2) {
		if (StringUtils.isNotBlank(g1.getOpenaireId())) {
			final int availables = BrokerConstants.MAX_NUMBER_OF_RELS - g1.getProjects().size();
			if (availables > 0) {
				if (g2.getProjects().size() <= availables) {
					g1.getProjects().addAll(g2.getProjects());
				} else {
					g1.getProjects().addAll(g2.getProjects().subList(0, availables));
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
