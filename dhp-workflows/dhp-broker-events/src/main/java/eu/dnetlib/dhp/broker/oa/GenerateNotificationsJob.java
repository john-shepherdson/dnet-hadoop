
package eu.dnetlib.dhp.broker.oa;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class GenerateNotificationsJob {

	private static final Logger log = LoggerFactory.getLogger(GenerateNotificationsJob.class);

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GenerateNotificationsJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/generate_notifications.json")));
		parser.parseArgument(args);

		final SparkConf conf = new SparkConf();

		final String eventsPath = parser.get("outputDir") + "/events";
		log.info("eventsPath: {}", eventsPath);

		final String notificationsPath = parser.get("outputDir") + "/notifications";
		log.info("notificationsPath: {}", notificationsPath);

		final String brokerApiBaseUrl = parser.get("brokerApiBaseUrl");
		log.info("brokerApiBaseUrl: {}", brokerApiBaseUrl);

		final SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		final LongAccumulator total = spark.sparkContext().longAccumulator("total_notifications");

		final long startTime = new Date().getTime();

		final List<Subscription> subscriptions = listSubscriptions(brokerApiBaseUrl);

		log.info("Number of subscriptions: " + subscriptions.size());

		if (subscriptions.size() > 0) {
			final Map<String, Map<String, List<ConditionParams>>> conditionsMap = prepareConditionsMap(subscriptions);

			log.info("ConditionsMap: " + new ObjectMapper().writeValueAsString(conditionsMap));

			final Encoder<NotificationGroup> ngEncoder = Encoders.bean(NotificationGroup.class);
			final Encoder<Notification> nEncoder = Encoders.bean(Notification.class);
			final Dataset<Notification> notifications = ClusterUtils
				.readPath(spark, eventsPath, Event.class)
				.map(
					(MapFunction<Event, NotificationGroup>) e -> generateNotifications(
						e, subscriptions, conditionsMap, startTime),
					ngEncoder)
				.flatMap((FlatMapFunction<NotificationGroup, Notification>) g -> g.getData().iterator(), nEncoder);

			ClusterUtils.save(notifications, notificationsPath, Notification.class, total);
		}
	}

	protected static Map<String, Map<String, List<ConditionParams>>> prepareConditionsMap(
		final List<Subscription> subscriptions) {
		final Map<String, Map<String, List<ConditionParams>>> map = new HashMap<>();
		subscriptions.forEach(s -> map.put(s.getSubscriptionId(), s.conditionsAsMap()));
		return map;
	}

	protected static NotificationGroup generateNotifications(final Event e,
		final List<Subscription> subscriptions,
		final Map<String, Map<String, List<ConditionParams>>> conditionsMap,
		final long date) {
		final List<Notification> list = subscriptions
			.stream()
			.filter(
				s -> StringUtils.isBlank(s.getTopic()) || s.getTopic().equals("*") || s.getTopic().equals(e.getTopic()))
			.filter(s -> verifyConditions(e.getMap(), conditionsMap.get(s.getSubscriptionId())))
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

		return !conditions.containsKey("targetSubjects")
			|| conditions
				.get("targetSubjects")
				.stream()
				.allMatch(c -> SubscriptionUtils.verifyListExact(map.getTargetSubjects(), c.getValue()));

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

}
