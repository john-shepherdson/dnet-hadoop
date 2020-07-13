
package eu.dnetlib.dhp.oa.dedup;

import java.util.Objects;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import eu.dnetlib.dhp.schema.oaf.Relation;

public class RelationAggregator extends Aggregator<Relation, Relation, Relation> {

	private static Relation ZERO = new Relation();

	@Override
	public Relation zero() {
		return ZERO;
	}

	@Override
	public Relation reduce(Relation b, Relation a) {
		return Objects.equals(a, ZERO) ? b : a;
	}

	@Override
	public Relation merge(Relation b, Relation a) {
		b.mergeFrom(a);
		return b;
	}

	@Override
	public Relation finish(Relation r) {
		return r;
	}

	@Override
	public Encoder<Relation> bufferEncoder() {
		return Encoders.bean(Relation.class);
	}

	@Override
	public Encoder<Relation> outputEncoder() {
		return Encoders.bean(Relation.class);
	}
}
