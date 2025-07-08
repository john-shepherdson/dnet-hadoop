
package eu.dnetlib.dhp.broker.oa_alerts;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.OaNotification;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.index.es.ConvertJSONWithId;
import eu.dnetlib.dhp.index.es.ESFeeder;
import eu.dnetlib.dhp.schema.mdstore.Provenance;
import eu.dnetlib.dhp.utils.DHPUtils;

public class IndexAlertNotificationsJob {

	private static final Logger log = LoggerFactory.getLogger(IndexAlertNotificationsJob.class);

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils
						.toString(IndexAlertNotificationsJob.class
								.getResourceAsStream("/eu/dnetlib/dhp/broker/oa_alert/index_alert_notifications.json")));
		parser.parseArgument(args);

		final SparkConf conf = new SparkConf();

		final String notificationsPath = parser.get("path");
		log.info("notificationsPath: {}", notificationsPath);

		final String index = parser.get("index");
		log.info("index: {}", index);

		final String indexHost = parser.get("esHost");
		log.info("indexHost: {}", indexHost);

		final String dsId = DHPUtils.MAPPER.readValue(parser.get("provenance"), Provenance.class).getDatasourceId();
		log.info("dsId: {}", dsId);

		final String brokerApiBaseUrl = parser.get("brokerApiBaseUrl");
		log.info("brokerApiBaseUrl: {}", brokerApiBaseUrl);

		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		final Long date = ClusterUtils
				.readPath(spark, notificationsPath, OaNotification.class)
				.first()
				.getDate();

		log.info("*** Start indexing");
		try (final ESFeeder feeder = new ESFeeder(indexHost)) {
			final FileSystem fileSystem = FileSystem.get(new Configuration());
			final List<Path> files = ClusterUtils.listFiles(notificationsPath, fileSystem, ".gz");
			feeder.parallelBulkIndex(files, 4, fileSystem, new ConvertJSONWithId("\"notificationId\":\"((\\d|\\w)*)\"", index));
			feeder.refreshIndex(index);
		}

		log.info("*** Deleting old notifications");
		final String message = deleteOldAlertNotifications(brokerApiBaseUrl, dsId, date - 1000);
		log.info("*** Deleted notifications: {}", message);

		log.info("*** sendNotifications (emails, ...)");
		sendAlertNotifications(brokerApiBaseUrl, dsId);
		log.info("*** ALL done.");

	}

	private static String deleteOldAlertNotifications(final String brokerApiBaseUrl, final String dsId, final long l) throws Exception {
		// TODO Complete the implementation of the remote method
		final String url = brokerApiBaseUrl + "/api/notifications/byDate/0/" + l + "?type=alert&dsId=" + dsId;

		final HttpDelete req = new HttpDelete(url);

		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			try (final CloseableHttpResponse response = client.execute(req)) {
				return IOUtils.toString(response.getEntity().getContent());
			}
		}
	}

	private static String sendAlertNotifications(final String brokerApiBaseUrl, final String dsId) throws IOException {
		final String url = brokerApiBaseUrl + "/api/openaire-alerts/notifications/sendMailForNotifications?dsId=" + dsId;
		final HttpGet req = new HttpGet(url);

		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			try (final CloseableHttpResponse response = client.execute(req)) {
				return IOUtils.toString(response.getEntity().getContent());
			}
		}
	}

}
