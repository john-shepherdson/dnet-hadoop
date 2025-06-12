
package eu.dnetlib.dhp.broker.oa;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Notification;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.ESIndexer;

public class IndexNotificationsJob {

	private static final Logger log = LoggerFactory.getLogger(IndexNotificationsJob.class);

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils
						.toString(IndexNotificationsJob.class
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

		final Long date = ClusterUtils
				.readPath(spark, notificationsPath, Notification.class)
				.first()
				.getDate();

		final Dataset<Notification> dataset = ClusterUtils
				.readPath(spark, notificationsPath, Notification.class);

		final ESIndexer indexer = new ESIndexer(indexHost, index, esBatchWriteRetryCount, esBatchWriteRetryWait, esBatchSizeEntries, esNodesWanOnly);

		indexer.performIndex(dataset, "notificationId");

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

}
