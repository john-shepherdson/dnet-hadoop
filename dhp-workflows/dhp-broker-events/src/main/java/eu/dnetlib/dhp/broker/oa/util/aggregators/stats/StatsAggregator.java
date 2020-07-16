
package eu.dnetlib.dhp.broker.oa.util.aggregators.stats;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import eu.dnetlib.dhp.broker.model.Event;

public class StatsAggregator extends Aggregator<Event, DatasourceStats, DatasourceStats> {

	/**
	 *
	 */
	private static final long serialVersionUID = 6652105853037330529L;

	@Override
	public DatasourceStats zero() {
		return new DatasourceStats();
	}

	@Override
	public DatasourceStats reduce(final DatasourceStats stats, final Event e) {
		stats.setId(e.getMap().getTargetDatasourceId());
		stats.setName(e.getMap().getTargetDatasourceName());
		stats.setType(e.getMap().getTargetDatasourceType());
		stats.incrementTopic(e.getTopic(), 1l);
		return stats;
	}

	@Override
	public DatasourceStats merge(final DatasourceStats stats0, final DatasourceStats stats1) {
		if (StringUtils.isBlank(stats0.getId())) {
			stats0.setId(stats1.getId());
			stats0.setName(stats1.getName());
			stats0.setType(stats1.getType());
		}
		stats1.getTopics().entrySet().forEach(e -> stats0.incrementTopic(e.getKey(), e.getValue()));
		return stats0;
	}

	@Override
	public Encoder<DatasourceStats> bufferEncoder() {
		return Encoders.bean(DatasourceStats.class);

	}

	@Override
	public DatasourceStats finish(final DatasourceStats stats) {
		return stats;
	}

	@Override
	public Encoder<DatasourceStats> outputEncoder() {
		return Encoders.bean(DatasourceStats.class);

	}
}
