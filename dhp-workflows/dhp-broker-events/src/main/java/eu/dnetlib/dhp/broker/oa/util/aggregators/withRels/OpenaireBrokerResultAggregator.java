
package eu.dnetlib.dhp.broker.oa.util.aggregators.withRels;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import eu.dnetlib.broker.objects.OpenaireBrokerResult;
import scala.Tuple2;

public class OpenaireBrokerResultAggregator<T>
	extends Aggregator<Tuple2<OpenaireBrokerResult, T>, OpenaireBrokerResult, OpenaireBrokerResult> {

	/**
	 *
	 */
	private static final long serialVersionUID = -3687878788861013488L;

	@Override
	public OpenaireBrokerResult zero() {
		return new OpenaireBrokerResult();
	}

	@Override
	public OpenaireBrokerResult finish(final OpenaireBrokerResult g) {
		return g;
	}

	@Override
	public OpenaireBrokerResult reduce(final OpenaireBrokerResult g, final Tuple2<OpenaireBrokerResult, T> t) {
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
		}
		return g;

	}

	@Override
	public OpenaireBrokerResult merge(final OpenaireBrokerResult g1, final OpenaireBrokerResult g2) {
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
	public Encoder<OpenaireBrokerResult> bufferEncoder() {
		return Encoders.bean(OpenaireBrokerResult.class);
	}

	@Override
	public Encoder<OpenaireBrokerResult> outputEncoder() {
		return Encoders.bean(OpenaireBrokerResult.class);
	}

}
