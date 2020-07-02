
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import scala.Tuple2;

public class AddDatasourceTypeAggregator
	extends Aggregator<Tuple2<OaBrokerMainEntity, SimpleDatasourceInfo>, OaBrokerMainEntity, OaBrokerMainEntity> {

	/**
	 *
	 */
	private static final long serialVersionUID = 8788588975496014728L;

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
		final Tuple2<OaBrokerMainEntity, SimpleDatasourceInfo> t) {
		final OaBrokerMainEntity res = StringUtils.isNotBlank(g.getOpenaireId()) ? g : t._1;
		if (t._2 != null && StringUtils.isNotBlank(t._2.getType())) {
			res.setCollectedFromType(t._2.getType());
		}
		return res;

	}

	@Override
	public OaBrokerMainEntity merge(final OaBrokerMainEntity g1, final OaBrokerMainEntity g2) {
		if (StringUtils.isNotBlank(g1.getOpenaireId()) && StringUtils.isNotBlank(g1.getCollectedFromType())) {
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
