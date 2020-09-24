
package eu.dnetlib.dhp.broker.oa.util.aggregators.subset;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.util.EventGroup;

public class EventSubsetAggregator extends Aggregator<Event, EventGroup, EventGroup> {

	/**
	 *
	 */
	private static final long serialVersionUID = -678071078823059805L;

	private final int maxEventsForTopic;

	public EventSubsetAggregator(final int maxEventsForTopic) {
		this.maxEventsForTopic = maxEventsForTopic;
	}

	@Override
	public EventGroup zero() {
		return new EventGroup();
	}

	@Override
	public EventGroup reduce(final EventGroup g, final Event e) {
		if (g.getData().size() < maxEventsForTopic) {
			g.getData().add(e);
		}
		return g;
	}

	@Override
	public EventGroup merge(final EventGroup g0, final EventGroup g1) {
		final int missing = maxEventsForTopic - g0.getData().size();

		if (missing > 0) {
			if (g1.getData().size() < missing) {
				g0.getData().addAll(g1.getData());
			} else {
				g0.getData().addAll(g1.getData().subList(0, missing));
			}
		}

		return g0;
	}

	@Override
	public EventGroup finish(final EventGroup g) {
		return g;
	}

	@Override
	public Encoder<EventGroup> outputEncoder() {
		return Encoders.bean(EventGroup.class);
	}

	@Override
	public Encoder<EventGroup> bufferEncoder() {
		return Encoders.bean(EventGroup.class);
	}

}
