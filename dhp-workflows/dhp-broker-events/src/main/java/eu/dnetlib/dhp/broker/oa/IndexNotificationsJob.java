
package eu.dnetlib.dhp.broker.oa;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.OaNotification;
import eu.dnetlib.dhp.broker.oa.util.BrokerIndexClient;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.index.es.ConvertJSONWithId;

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

		final String brokerApiBaseUrl = parser.get("brokerApiBaseUrl");
		log.info("brokerApiBaseUrl: {}", brokerApiBaseUrl);

		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		final Long date = ClusterUtils
				.readPath(spark, notificationsPath, OaNotification.class)
				.first()
				.getDate();

		try (final BrokerIndexClient feeder = new BrokerIndexClient(indexHost)) {
			final FileSystem fileSystem = FileSystem.get(new Configuration());
			final List<Path> files = ClusterUtils.listFiles(notificationsPath, fileSystem, ".gz");

			log.info("*** Start indexing");
			feeder.parallelBulkIndex(files, 4, fileSystem, new ConvertJSONWithId("\"notificationId\":\"((\\d|\\w|-)*)\"", index));

			log.info("*** Deleting old notifications");
			feeder.deleteUsingDateBefore(index, "date", date - 1000, true);

			feeder.refreshIndex(index);
		}

		if (StringUtils.isBlank(brokerApiBaseUrl) || !StringUtils.startsWith(brokerApiBaseUrl, "http")) {
			log.warn("brokerApiBaseUrl is not set, skipping sendNotifications");
		} else {
			log.info("*** sendNotifications (emails, ...)");
			sendNotifications(brokerApiBaseUrl, date - 1000);
		}

		log.info("*** ALL done.");

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
