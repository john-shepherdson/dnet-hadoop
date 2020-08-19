
package eu.dnetlib.dhp.broker.oa;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.util.LongAccumulator;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.EventGroup;
import eu.dnetlib.dhp.broker.oa.util.aggregators.subset.EventSubsetAggregator;

public class IndexEventSubsetJob {

	private static final Logger log = LoggerFactory.getLogger(IndexEventSubsetJob.class);

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					IndexEventSubsetJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/index_event_subset.json")));
		parser.parseArgument(args);

		final SparkConf conf = new SparkConf();

		final String eventsPath = parser.get("workingPath") + "/events";
		log.info("eventsPath: {}", eventsPath);

		final String index = parser.get("index");
		log.info("index: {}", index);

		final String indexHost = parser.get("esHost");
		log.info("indexHost: {}", indexHost);

		final int maxEventsForTopic = NumberUtils.toInt(parser.get("maxEventsForTopic"));
		log.info("maxEventsForTopic: {}", maxEventsForTopic);

		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		final TypedColumn<Event, EventGroup> aggr = new EventSubsetAggregator(maxEventsForTopic).toColumn();

		final LongAccumulator total = spark.sparkContext().longAccumulator("total_indexed");

		final long now = new Date().getTime();

		final Dataset<Event> subset = ClusterUtils
			.readPath(spark, eventsPath, Event.class)
			.groupByKey(e -> e.getTopic() + '@' + e.getMap().getTargetDatasourceId(), Encoders.STRING())
			.agg(aggr)
			.map(t -> t._2, Encoders.bean(EventGroup.class))
			.flatMap(g -> g.getData().iterator(), Encoders.bean(Event.class));

		final JavaRDD<String> inputRdd = subset
			.map(e -> prepareEventForIndexing(e, now, total), Encoders.STRING())
			.javaRDD();

		final Map<String, String> esCfg = new HashMap<>();
		// esCfg.put("es.nodes", "10.19.65.51, 10.19.65.52, 10.19.65.53, 10.19.65.54");

		esCfg.put("es.index.auto.create", "false");
		esCfg.put("es.nodes", indexHost);
		esCfg.put("es.mapping.id", "eventId"); // THE PRIMARY KEY
		esCfg.put("es.batch.write.retry.count", "8");
		esCfg.put("es.batch.write.retry.wait", "60s");
		esCfg.put("es.batch.size.entries", "200");
		esCfg.put("es.nodes.wan.only", "true");

		JavaEsSpark.saveJsonToEs(inputRdd, index, esCfg);
	}

	private static String prepareEventForIndexing(final Event e, final long creationDate, final LongAccumulator acc)
		throws JsonProcessingException {
		acc.add(1);

		e.setCreationDate(creationDate);
		e.setExpiryDate(Long.MAX_VALUE);

		return new ObjectMapper().writeValueAsString(e);
	}

}
