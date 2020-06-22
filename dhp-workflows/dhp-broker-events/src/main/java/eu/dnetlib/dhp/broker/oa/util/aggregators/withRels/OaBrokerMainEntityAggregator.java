
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import scala.Tuple2;

public class OaBrokerMainEntityAggregator<T>
	extends Aggregator<Tuple2<OaBrokerMainEntity, T>, OaBrokerMainEntity, OaBrokerMainEntity> {

	/**
	 *
	 */
	private static final long serialVersionUID = -3687878788861013488L;

	@Override
	public OaBrokerMainEntity zero() {
		return new OaBrokerMainEntity();
	}

	@Override
	public OaBrokerMainEntity finish(final OaBrokerMainEntity g) {
		return g;
	}

	@Override
	public OaBrokerMainEntity reduce(final OaBrokerMainEntity g, final Tuple2<OaBrokerMainEntity, T> t) {
		if (g.getOriginalId() == null) {
			return t._1;
		} else if (t._2 instanceof RelatedSoftware) {
			g.getSoftwares().add(((RelatedSoftware) t._2).getRelSoftware());
		} else if (t._2 instanceof RelatedDataset) {
			g.getDatasets().add(((RelatedDataset) t._2).getRelDataset());
		} else if (t._2 instanceof RelatedPublication) {
			g.getPublications().add(((RelatedPublication) t._2).getRelPublication());
		} else if (t._2 instanceof RelatedProject) {
			g.getProjects().add(((RelatedProject) t._2).getRelProject());
		} else {
			throw new RuntimeException("Invalid Object: " + t._2.getClass());
		}
		return g;

	}

	@Override
	public OaBrokerMainEntity merge(final OaBrokerMainEntity g1, final OaBrokerMainEntity g2) {
		if (g1.getOriginalId() != null) {
			g1.getSoftwares().addAll(g2.getSoftwares());
			g1.getDatasets().addAll(g2.getDatasets());
			g1.getPublications().addAll(g2.getPublications());
			g1.getProjects().addAll(g2.getProjects());
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
