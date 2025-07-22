
package eu.dnetlib.dhp.broker.oa;

import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.EventGroup;
import eu.dnetlib.dhp.broker.oa.util.aggregators.subset.EventSubsetAggregator;
import scala.Tuple2;

public class GenerateEventSubsetJob {

	private static final Logger log = LoggerFactory.getLogger(GenerateEventSubsetJob.class);

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils
						.toString(GenerateEventSubsetJob.class
								.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/generate_event_subset.json")));
		parser.parseArgument(args);

		final SparkConf conf = new SparkConf();

		final String eventsPath = parser.get("outputDir") + "/events";
		log.info("eventsPath: {}", eventsPath);

		final String eventsSubsetPath = parser.get("outputDir") + "/events_subset";
		log.info("eventsSubsetPath: {}", eventsSubsetPath);

		final int maxEventsForTopic = NumberUtils.toInt(parser.get("maxEventsForTopic"));
		log.info("maxEventsForTopic: {}", maxEventsForTopic);

		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		final TypedColumn<Event, EventGroup> aggr = new EventSubsetAggregator(maxEventsForTopic).toColumn();

		final long now = new Date().getTime();

		final LongAccumulator total = spark.sparkContext().longAccumulator("total_events_subset");

		log.info("*** Prepare subset for indexing");
		final Dataset<Event> subset = ClusterUtils
				.readPath(spark, eventsPath, Event.class)
				.groupByKey((MapFunction<Event, String>) e -> e.getTopic() + '@' + e.getMap().getTargetDatasourceId(), Encoders.STRING())
				.agg(aggr)
				.map((MapFunction<Tuple2<String, EventGroup>, EventGroup>) t -> t._2, Encoders.bean(EventGroup.class))
				.flatMap((FlatMapFunction<EventGroup, Event>) g -> g.getData().iterator(), Encoders.bean(Event.class))
				.map((MapFunction<Event, Event>) e -> prepareEventForIndexing(e, now), Encoders.bean(Event.class));

		ClusterUtils.save(subset, eventsSubsetPath, Event.class, total);

		log.info("*** ALL DONE");

	}

	private static Event prepareEventForIndexing(final Event e, final long creationDate)
			throws JsonProcessingException {

		e.setCreationDate(creationDate);
		e.setExpiryDate(Long.MAX_VALUE);

		return e;
	}

}
