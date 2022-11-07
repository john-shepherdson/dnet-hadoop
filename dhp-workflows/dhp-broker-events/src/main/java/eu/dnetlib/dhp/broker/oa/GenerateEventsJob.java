
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.EventFinder;
import eu.dnetlib.dhp.broker.oa.util.EventGroup;
import eu.dnetlib.dhp.broker.oa.util.aggregators.simple.ResultGroup;

public class GenerateEventsJob {

	private static final Logger log = LoggerFactory.getLogger(GenerateEventsJob.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GenerateEventsJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/generate_events.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String workingDir = parser.get("workingDir");
		log.info("workingDir: {}", workingDir);

		final String eventsPath = parser.get("outputDir") + "/events";
		log.info("eventsPath: {}", eventsPath);

		final Set<String> dsIdWhitelist = ClusterUtils.parseParamAsList(parser, "datasourceIdWhitelist");
		log.info("datasourceIdWhitelist: {}", StringUtils.join(dsIdWhitelist, ","));

		final Set<String> dsTypeWhitelist = ClusterUtils.parseParamAsList(parser, "datasourceTypeWhitelist");
		log.info("datasourceTypeWhitelist: {}", StringUtils.join(dsTypeWhitelist, ","));

		final Set<String> dsIdBlacklist = ClusterUtils.parseParamAsList(parser, "datasourceIdBlacklist");
		log.info("datasourceIdBlacklist: {}", StringUtils.join(dsIdBlacklist, ","));

		final Set<String> topicWhitelist = ClusterUtils.parseParamAsList(parser, "topicWhitelist");
		log.info("topicWhitelist: {}", StringUtils.join(topicWhitelist, ","));

		final SparkConf conf = new SparkConf();

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, eventsPath);

			final Map<String, LongAccumulator> accumulators = prepareAccumulators(spark.sparkContext());

			final LongAccumulator total = spark.sparkContext().longAccumulator("total_events");

			final Dataset<ResultGroup> groups = ClusterUtils
				.readPath(spark, workingDir + "/duplicates", ResultGroup.class);

			final Dataset<Event> dataset = groups
				.map(
					(MapFunction<ResultGroup, EventGroup>) g -> EventFinder
						.generateEvents(g, dsIdWhitelist, dsIdBlacklist, dsTypeWhitelist, topicWhitelist, accumulators),
					Encoders
						.bean(EventGroup.class))
				.flatMap((FlatMapFunction<EventGroup, Event>) g -> g.getData().iterator(), Encoders.bean(Event.class));

			ClusterUtils.save(dataset, eventsPath, Event.class, total);

		});

	}

	public static Map<String, LongAccumulator> prepareAccumulators(final SparkContext sc) {

		return EventFinder
			.getMatchers()
			.stream()
			.map(UpdateMatcher::accumulatorName)
			.distinct()
			.collect(Collectors.toMap(s -> s, s -> sc.longAccumulator(s)));

	}

}
