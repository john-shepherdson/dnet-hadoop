
package eu.dnetlib.dhp.broker.oa;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.ConditionParams;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.model.MappedFields;
import eu.dnetlib.dhp.broker.model.Notification;
import eu.dnetlib.dhp.broker.model.Subscription;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.NotificationGroup;
import eu.dnetlib.dhp.broker.oa.util.SubscriptionUtils;

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

		final String eventsPath = parser.get("outputDir") + "/events";
		log.info("eventsPath: {}", eventsPath);

		final String index = parser.get("index");
		log.info("index: {}", index);

		final String indexHost = parser.get("esHost");
		log.info("indexHost: {}", indexHost);

		final String brokerApiBaseUrl = parser.get("brokerApiBaseUrl");
		log.info("brokerApiBaseUrl: {}", brokerApiBaseUrl);

		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		final LongAccumulator total = spark.sparkContext().longAccumulator("total_indexed");

		final long startTime = new Date().getTime();

		final List<Subscription> subscriptions = listSubscriptions(brokerApiBaseUrl);

		log.info("Number of subscriptions: " + subscriptions.size());

		if (subscriptions.size() > 0) {
			final Dataset<Notification> notifications = ClusterUtils
				.readPath(spark, eventsPath, Event.class)
				.map(e -> generateNotifications(e, subscriptions, startTime), Encoders.bean(NotificationGroup.class))
				.flatMap(g -> g.getData().iterator(), Encoders.bean(Notification.class));

			final JavaRDD<String> inputRdd = notifications
				.map(n -> prepareForIndexing(n, total), Encoders.STRING())
				.javaRDD();

			final Map<String, String> esCfg = new HashMap<>();
			// esCfg.put("es.nodes", "10.19.65.51, 10.19.65.52, 10.19.65.53, 10.19.65.54");

			esCfg.put("es.index.auto.create", "false");
			esCfg.put("es.nodes", indexHost);
			esCfg.put("es.mapping.id", "notificationId"); // THE PRIMARY KEY
			esCfg.put("es.batch.write.retry.count", "8");
			esCfg.put("es.batch.write.retry.wait", "60s");
			esCfg.put("es.batch.size.entries", "200");
			esCfg.put("es.nodes.wan.only", "true");

			log.info("*** Start indexing");
			JavaEsSpark.saveJsonToEs(inputRdd, index, esCfg);
			log.info("*** End indexing");

			log.info("*** Deleting old notifications");
			final String message = deleteOldNotifications(brokerApiBaseUrl, startTime - 1000);
			log.info("*** Deleted notifications: " + message);

			log.info("*** sendNotifications (emails, ...)");
			sendNotifications(brokerApiBaseUrl, startTime - 1000);
			log.info("*** ALL done.");

		}
	}

	private static NotificationGroup generateNotifications(final Event e,
		final List<Subscription> subscriptions,
		final long date) {
		final List<Notification> list = subscriptions
			.stream()
			.filter(
				s -> StringUtils.isBlank(s.getTopic()) || s.getTopic().equals("*") || s.getTopic().equals(e.getTopic()))
			.filter(s -> verifyConditions(e.getMap(), s.conditionsAsMap()))
			.map(s -> generateNotification(s, e, date))
			.collect(Collectors.toList());

		return new NotificationGroup(list);
	}

	private static Notification generateNotification(final Subscription s, final Event e, final long date) {
		final Notification n = new Notification();
		n.setNotificationId("ntf-" + DigestUtils.md5Hex(s.getSubscriptionId() + "@@@" + e.getEventId()));
		n.setSubscriptionId(s.getSubscriptionId());
		n.setEventId(e.getEventId());
		n.setProducerId(e.getProducerId());
		n.setTopic(e.getTopic());
		n.setPayload(e.getPayload());
		n.setMap(e.getMap());
		n.setDate(date);
		return n;
	}

	private static boolean verifyConditions(final MappedFields map,
		final Map<String, List<ConditionParams>> conditions) {
		if (conditions.containsKey("targetDatasourceName")
			&& !SubscriptionUtils
				.verifyExact(map.getTargetDatasourceName(), conditions.get("targetDatasourceName").get(0).getValue())) {
			return false;
		}

		if (conditions.containsKey("trust")
			&& !SubscriptionUtils
				.verifyFloatRange(
					map.getTrust(), conditions.get("trust").get(0).getValue(),
					conditions.get("trust").get(0).getOtherValue())) {
			return false;
		}

		if (conditions.containsKey("targetDateofacceptance") && !conditions
			.get("targetDateofacceptance")
			.stream()
			.anyMatch(
				c -> SubscriptionUtils
					.verifyDateRange(map.getTargetDateofacceptance(), c.getValue(), c.getOtherValue()))) {
			return false;
		}

		if (conditions.containsKey("targetResultTitle")
			&& !conditions
				.get("targetResultTitle")
				.stream()
				.anyMatch(c -> SubscriptionUtils.verifySimilar(map.getTargetResultTitle(), c.getValue()))) {
			return false;
		}

		if (conditions.containsKey("targetAuthors")
			&& !conditions
				.get("targetAuthors")
				.stream()
				.allMatch(c -> SubscriptionUtils.verifyListSimilar(map.getTargetAuthors(), c.getValue()))) {
			return false;
		}

		if (conditions.containsKey("targetSubjects")
			&& !conditions
				.get("targetSubjects")
				.stream()
				.allMatch(c -> SubscriptionUtils.verifyListExact(map.getTargetSubjects(), c.getValue()))) {
			return false;
		}

		return true;

	}

	private static List<Subscription> listSubscriptions(final String brokerApiBaseUrl) throws Exception {
		final String url = brokerApiBaseUrl + "/api/subscriptions";
		final HttpGet req = new HttpGet(url);

		final ObjectMapper mapper = new ObjectMapper();

		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			try (final CloseableHttpResponse response = client.execute(req)) {
				final String s = IOUtils.toString(response.getEntity().getContent());
				return mapper
					.readValue(s, mapper.getTypeFactory().constructCollectionType(List.class, Subscription.class));
			}
		}
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
