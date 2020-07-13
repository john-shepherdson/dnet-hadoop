
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.TypedColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.stats.DatasourceStats;
import eu.dnetlib.dhp.broker.oa.util.aggregators.stats.StatsAggregator;

public class GenerateStatsJob {

	private static final Logger log = LoggerFactory.getLogger(GenerateStatsJob.class);

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					IndexOnESJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/common_params.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final SparkConf conf = new SparkConf();

		final String eventsPath = parser.get("workingPath") + "/events";
		log.info("eventsPath: {}", eventsPath);

		final String statsPath = parser.get("workingPath") + "/stats";
		log.info("stats: {}", statsPath);

		final TypedColumn<Event, DatasourceStats> aggr = new StatsAggregator().toColumn();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			final Dataset<DatasourceStats> stats = ClusterUtils
				.readPath(spark, eventsPath, Event.class)
				.groupByKey(e -> e.getMap().getTargetDatasourceId(), Encoders.STRING())
				.agg(aggr)
				.map(t -> t._2, Encoders.bean(DatasourceStats.class));

			ClusterUtils.save(stats, statsPath, DatasourceStats.class, null);
		});
	}

}
