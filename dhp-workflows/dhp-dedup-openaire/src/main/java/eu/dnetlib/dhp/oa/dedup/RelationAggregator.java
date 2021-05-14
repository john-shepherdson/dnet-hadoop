
package eu.dnetlib.dhp.oa.dedup;

import java.util.Objects;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import eu.dnetlib.dhp.schema.oaf.Relation;

public class RelationAggregator extends Aggregator<Relation, Relation, Relation> {

	private static final Relation ZERO = new Relation();

	@Override
	public Relation zero() {
		return ZERO;
	}

	@Override
	public Relation reduce(Relation b, Relation a) {
		return mergeRel(b, a);
	}

	@Override
	public Relation merge(Relation b, Relation a) {
		return mergeRel(b, a);
	}

	@Override
	public Relation finish(Relation r) {
		return r;
	}

	private Relation mergeRel(Relation b, Relation a) {
		if (Objects.equals(b, ZERO)) {
			return a;
		}
		if (Objects.equals(a, ZERO)) {
			return b;
		}

		b.mergeFrom(a);
		return b;
	}

	@Override
	public Encoder<Relation> bufferEncoder() {
		return Encoders.kryo(Relation.class);
	}

	@Override
	public Encoder<Relation> outputEncoder() {
		return Encoders.kryo(Relation.class);
	}
}
