
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import scala.Tuple2;

public class ResultWithRelationsAggregator<T>
	extends Aggregator<Tuple2<ResultWithRelations, T>, ResultWithRelations, ResultWithRelations> {

	/**
	 *
	 */
	private static final long serialVersionUID = -3687878788861013488L;

	@Override
	public ResultWithRelations zero() {
		return new ResultWithRelations();
	}

	@Override
	public ResultWithRelations finish(final ResultWithRelations g) {
		return g;
	}

	@Override
	public ResultWithRelations reduce(final ResultWithRelations g, final Tuple2<ResultWithRelations, T> t) {
		if (g.getResult() == null) {
			return t._1;
		} else if (t._2 instanceof RelatedSoftware) {
			g.getSoftwares().add((RelatedSoftware) t._2);
		} else if (t._2 instanceof RelatedDataset) {
			g.getDatasets().add((RelatedDataset) t._2);
		} else if (t._2 instanceof RelatedPublication) {
			g.getPublications().add((RelatedPublication) t._2);
		} else if (t._2 instanceof RelatedProject) {
			g.getProjects().add((RelatedProject) t._2);
		}
		return g;

	}

	@Override
	public ResultWithRelations merge(final ResultWithRelations g1, final ResultWithRelations g2) {
		if (g1.getResult() != null) {
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
	public Encoder<ResultWithRelations> bufferEncoder() {
		return Encoders.kryo(ResultWithRelations.class);
	}

	@Override
	public Encoder<ResultWithRelations> outputEncoder() {
		return Encoders.kryo(ResultWithRelations.class);
	}

}
