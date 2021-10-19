
package eu.dnetlib.dhp.broker.oa;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Notification;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;

public class IndexNotificationsJob {

	private static final Logger log = LoggerFactory.getLogger(IndexNotificationsJob.class);

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					IndexNotificationsJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/index_notifications.json")));
		parser.parseArgument(args);

		final SparkConf conf = new SparkConf();

		final String notificationsPath = parser.get("outputDir") + "/notifications";
		log.info("notificationsPath: {}", notificationsPath);

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

		final String brokerApiBaseUrl = parser.get("brokerApiBaseUrl");
		log.info("brokerApiBaseUrl: {}", brokerApiBaseUrl);

		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		final LongAccumulator total = spark.sparkContext().longAccumulator("total_indexed");

		final Long date = ClusterUtils
			.readPath(spark, notificationsPath, Notification.class)
			.first()
			.getDate();

		final JavaRDD<String> toIndexRdd = ClusterUtils
			.readPath(spark, notificationsPath, Notification.class)
			.map((MapFunction<Notification, String>) n -> prepareForIndexing(n, total), Encoders.STRING())
			.javaRDD();

		final Map<String, String> esCfg = new HashMap<>();

		esCfg.put("es.index.auto.create", "false");
		esCfg.put("es.nodes", indexHost);
		esCfg.put("es.mapping.id", "notificationId"); // THE PRIMARY KEY
		esCfg.put("es.batch.write.retry.count", esBatchWriteRetryCount);
		esCfg.put("es.batch.write.retry.wait", esBatchWriteRetryWait);
		esCfg.put("es.batch.size.entries", esBatchSizeEntries);
		esCfg.put("es.nodes.wan.only", esNodesWanOnly);

		log.info("*** Start indexing");
		JavaEsSpark.saveJsonToEs(toIndexRdd, index, esCfg);
		log.info("*** End indexing");

		log.info("*** Deleting old notifications");
		final String message = deleteOldNotifications(brokerApiBaseUrl, date - 1000);
		log.info("*** Deleted notifications: {}", message);

		log.info("*** sendNotifications (emails, ...)");
		sendNotifications(brokerApiBaseUrl, date - 1000);
		log.info("*** ALL done.");

	}

	private static String deleteOldNotifications(final String brokerApiBaseUrl, final long l) throws Exception {
		final String url = brokerApiBaseUrl + "/api/notifications/byDate/0/" + l;
		final HttpDelete req = new HttpDelete(url);

		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			try (final CloseableHttpResponse response = client.execute(req)) {
				return IOUtils.toString(response.getEntity().getContent());
			}
		}
	}

	private static String sendNotifications(final String brokerApiBaseUrl, final long l) throws IOException {
		final String url = brokerApiBaseUrl + "/api/openaireBroker/notifications/send/" + l;
		final HttpGet req = new HttpGet(url);

		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			try (final CloseableHttpResponse response = client.execute(req)) {
				return IOUtils.toString(response.getEntity().getContent());
			}
		}
	}

	private static String prepareForIndexing(final Notification n, final LongAccumulator acc)
		throws JsonProcessingException {
		acc.add(1);
		return new ObjectMapper().writeValueAsString(n);
	}

}
