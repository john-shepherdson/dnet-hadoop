
package eu.dnetlib.dhp.broker.oa;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.ESIndexer;
import eu.dnetlib.dhp.broker.oa.util.EventGroup;
import eu.dnetlib.dhp.broker.oa.util.aggregators.subset.EventSubsetAggregator;
import scala.Tuple2;

public class IndexEventSubsetJob {

	private static final Logger log = LoggerFactory.getLogger(IndexEventSubsetJob.class);

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils
						.toString(IndexEventSubsetJob.class
								.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/index_event_subset.json")));
		parser.parseArgument(args);

		final SparkConf conf = new SparkConf();

		final String eventsPath = parser.get("outputDir") + "/events";
		log.info("eventsPath: {}", eventsPath);

		final String index = parser.get("index");
		log.info("index: {}", index);

		final String indexHost = parser.get("esHost");
		log.info("indexHost: {}", indexHost);

		final String esBatchWriteRetryCount = parser.get("esBatchWriteRetryCount");
		log.info("esBatchWriteRetryCount: {}", esBatchWriteRetryCount);

		final String esBatchWriteRetryWait = parser.get("esBatchWriteRetryWait");
		log.info("esBatchWriteRetryWait: {}", esBatchWriteRetryWait);

		final String esBatchSizeEntries = parser.get("esBatchSizeEntries");
		log.info("esBatchSizeEntries: {}", esBatchSizeEntries);

		final String esNodesWanOnly = parser.get("esNodesWanOnly");
		log.info("esNodesWanOnly: {}", esNodesWanOnly);

		final int maxEventsForTopic = NumberUtils.toInt(parser.get("maxEventsForTopic"));
		log.info("maxEventsForTopic: {}", maxEventsForTopic);

		final String brokerApiBaseUrl = parser.get("brokerApiBaseUrl");
		log.info("brokerApiBaseUrl: {}", brokerApiBaseUrl);

		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		final TypedColumn<Event, EventGroup> aggr = new EventSubsetAggregator(maxEventsForTopic).toColumn();

		final long now = new Date().getTime();

		final Dataset<Event> subset = ClusterUtils
				.readPath(spark, eventsPath, Event.class)
				.groupByKey((MapFunction<Event, String>) e -> e.getTopic() + '@' + e.getMap().getTargetDatasourceId(), Encoders.STRING())
				.agg(aggr)
				.map((MapFunction<Tuple2<String, EventGroup>, EventGroup>) t -> t._2, Encoders.bean(EventGroup.class))
				.flatMap((FlatMapFunction<EventGroup, Event>) g -> g.getData().iterator(), Encoders.bean(Event.class))
				.map((MapFunction<Event, Event>) e -> prepareEventForIndexing(e, now), Encoders.bean(Event.class));

		final ESIndexer indexer = new ESIndexer(indexHost, index, esBatchWriteRetryCount, esBatchWriteRetryWait, esBatchSizeEntries, esNodesWanOnly);

		indexer.performIndex(subset, "eventId");

		log.info("*** Deleting old events");
		final String message = deleteOldEvents(brokerApiBaseUrl, now - 1000);
		log.info("*** Deleted events: {}", message);

	}

	private static String deleteOldEvents(final String brokerApiBaseUrl, final long l) throws IOException {
		final String url = brokerApiBaseUrl + "/api/events/byCreationDate/0/" + l;
		final HttpDelete req = new HttpDelete(url);

		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			try (final CloseableHttpResponse response = client.execute(req)) {
				return IOUtils.toString(response.getEntity().getContent());
			}
		}

	}

	private static Event prepareEventForIndexing(final Event e, final long creationDate)
			throws JsonProcessingException {

		e.setCreationDate(creationDate);
		e.setExpiryDate(Long.MAX_VALUE);

		return e;
	}

}
