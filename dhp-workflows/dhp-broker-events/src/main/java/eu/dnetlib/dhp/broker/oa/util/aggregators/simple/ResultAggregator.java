
package eu.dnetlib.dhp.broker.oa.util.aggregators.simple;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

public class ResultAggregator extends Aggregator<Tuple2<OaBrokerMainEntity, Relation>, ResultGroup, ResultGroup> {

	/**
	 *
	 */
	private static final long serialVersionUID = -1492327874705585538L;

	@Override
	public ResultGroup zero() {
		return new ResultGroup();
	}

	@Override
	public ResultGroup reduce(final ResultGroup group, final Tuple2<OaBrokerMainEntity, Relation> t) {
		group.getData().add(t._1);
		return group;
	}

	@Override
	public ResultGroup merge(final ResultGroup g1, final ResultGroup g2) {
		g1.getData().addAll(g2.getData());
		return g1;
	}

	@Override
	public ResultGroup finish(final ResultGroup group) {
		return group;
	}

	@Override
	public Encoder<ResultGroup> bufferEncoder() {
		return Encoders.bean(ResultGroup.class);

	}

	@Override
	public Encoder<ResultGroup> outputEncoder() {
		return Encoders.bean(ResultGroup.class);

	}

}
