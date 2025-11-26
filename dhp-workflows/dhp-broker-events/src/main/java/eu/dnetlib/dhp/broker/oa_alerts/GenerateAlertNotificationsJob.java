
package eu.dnetlib.dhp.broker.oa_alerts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.util.LongAccumulator;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

import eu.dnetlib.broker.objects.alerts.ValidatorAlertMessage;
import eu.dnetlib.broker.objects.alerts.ValidatorErrorMessage;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.MapCondition;
import eu.dnetlib.dhp.broker.model.OaAlertMappedFields;
import eu.dnetlib.dhp.broker.model.OaAlertNotification;
import eu.dnetlib.dhp.broker.model.Subscription;
import eu.dnetlib.dhp.broker.oa.util.BrokerApiClient;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.stats.DatasourceStats;
import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.common.Constants;
import eu.dnetlib.dhp.common.SparkSessionSupport;
import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion;
import eu.dnetlib.dhp.schema.mdstore.MetadataRecord;
import eu.dnetlib.dhp.schema.mdstore.Provenance;
import eu.dnetlib.dhp.schema.mdstore.ValidationType;
import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.validator2.result_models.StandardValidationResult;

public class GenerateAlertNotificationsJob {

	private static final String TOPIC_PREFIX = "ALERT/";

	private static final Logger log = LoggerFactory.getLogger(GenerateAlertNotificationsJob.class);

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils
						.toString(GenerateAlertNotificationsJob.class
								.getResourceAsStream("/eu/dnetlib/dhp/broker/oa_alerts/generate_alert_notifications.json")));

		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
				.ofNullable(parser.get("isSparkSessionManaged"))
				.map(Boolean::valueOf)
				.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String mdstoreInputVersion = parser.get("mdstoreInputVersion");

		final Provenance provenance = DHPUtils.MAPPER.readValue(parser.get("provenance"), Provenance.class);
		final ApiDescriptor api = DHPUtils.MAPPER.readValue(parser.get("apidescriptor"), ApiDescriptor.class);

		final String dsId = provenance.getDatasourceId();
		log.info("dsId: {}", dsId);

		final String dsName = provenance.getDatasourceName();
		log.info("dsName: {}", dsName);

		final String compatibilityLevel = api.getCompatibilityLevel();
		log.info("compatibilityLevel: {}", compatibilityLevel);

		final MDStoreVersion mdstoreVersion = DHPUtils.MAPPER.readValue(mdstoreInputVersion, MDStoreVersion.class);
		final String inputPath = mdstoreVersion.getHdfsPath() + Constants.MDSTORE_DATA_PATH;
		log.info("inputPath: {}", inputPath);

		final String outputPath = ClusterUtils.pathForAlertNotifications(parser.get("outputDir"), api);
		log.info(outputPath);

		final String brokerApiBaseUrl = parser.get("brokerApiBaseUrl");
		log.info("brokerApiBaseUrl: {}", brokerApiBaseUrl);

		if (StringUtils.isAnyBlank(dsId, compatibilityLevel, inputPath, brokerApiBaseUrl)) { throw new RuntimeException("A required information is missing"); }

		final List<ValidationType> validationTypes = calculateValidationTypes(compatibilityLevel);

		if (validationTypes.isEmpty()) {
			log.warn("The compatibility is non managed by the validator engine");
		}

		final Set<String> topics = validationTypes.stream()
				.map(GenerateAlertNotificationsJob::asTopic)
				.collect(Collectors.toSet());

		topics.forEach(t -> log.info("topic: {}", t));

		final Subscription[] allSubscriptions = BrokerApiClient.listSubscriptions(brokerApiBaseUrl);

		final Map<String, List<Subscription>> validSubscriptions = Arrays.stream(allSubscriptions)
				.filter(s -> topics.contains(s.getTopic()))
				.filter(s -> extractDatasourceId(s).equalsIgnoreCase(dsId))
				.collect(Collectors.groupingBy(Subscription::getTopic));

		log.info("Number of valid subscriptions: {}/{}", validSubscriptions.values()
				.stream()
				.flatMap(List::stream)
				.count(), allSubscriptions.length);

		final Long date = new Date().getTime();
		log.info("date: {}", date);

		BrokerApiClient.clearAlertStats(brokerApiBaseUrl, dsId);

		SparkSessionSupport.runWithSparkSession(new SparkConf(), isSparkSessionManaged, spark -> {

			final List<Dataset<OaAlertNotification>> datasets = new ArrayList<>();

			for (final ValidationType type : validationTypes) {
				final String topic = asTopic(type);

				final Dataset<ValidatorAlertMessage> payloads = spark
						.read()
						.parquet(inputPath)
						.as(Encoders.bean(MetadataRecord.class))
						.filter((FilterFunction<MetadataRecord>) r -> r.getValidationResults() != null)
						.filter((FilterFunction<MetadataRecord>) r -> r.getValidationResults().containsKey(type))
						.map((MapFunction<MetadataRecord, ValidatorAlertMessage>) r -> generatePayload(r.getOriginalId(), extractTitle(r), dsId, dsName, r
								.getValidationResults()
								.get(type)), Encoders
										.bean(ValidatorAlertMessage.class));

				final long count = payloads.count();

				log.info("Number of events for topic {}: {}", topic, count);

				final DatasourceStats stats = new DatasourceStats();
				stats.setId(dsId);
				stats.setName(dsName);
				stats.setType("-"); // TODO
				stats.setTopic(topic);
				stats.setSize(count);

				BrokerApiClient.updateAlertStats(brokerApiBaseUrl, stats);

				if (validSubscriptions.size() > 0) {
					final Dataset<OaAlertNotification> alertDataset = payloads
							.flatMap((FlatMapFunction<ValidatorAlertMessage, OaAlertNotification>) p -> generateAlertNotifications(p, date, validSubscriptions
									.get(topic)), Encoders
											.bean(OaAlertNotification.class))
							.filter((FilterFunction<OaAlertNotification>) n -> StringUtils.isNotBlank(n.getPayload()));

					datasets.add(alertDataset);
				} else {
					datasets.add(spark.emptyDataset(Encoders.bean(OaAlertNotification.class)));
				}
			}

			final Dataset<OaAlertNotification> toSaveDataset = datasets.stream()
					.reduce(Dataset::union)
					.orElseGet(() -> spark.emptyDataset(Encoders.bean(OaAlertNotification.class)));

			final LongAccumulator total = spark.sparkContext().longAccumulator("total_alert_notifications");
			ClusterUtils.save(toSaveDataset, outputPath, OaAlertNotification.class, total);

			log.info("Number of notifications: {}", total.value());

		});
	}

	private static String asTopic(final ValidationType t) {
		return TOPIC_PREFIX + StringUtils.upperCase(t.toString());
	}

	private static String extractTitle(final MetadataRecord r) {
		try {
			for (final Object o : DocumentHelper.parseText(r.getBody()).selectNodes("//*[local-name() = 'title']")) {
				final String title = ((Node) o).getText();
				if (StringUtils.isNotBlank(title)) { return title; }
			}
		} catch (final Throwable e) {}

		return "-";
	}

	private static List<ValidationType> calculateValidationTypes(final String compatibilityLevel) {

		switch (compatibilityLevel) {
		case "openaire2.0":
			return Arrays.asList(ValidationType.openaire2_0);
		case "openaire3.0":
			return Arrays.asList(ValidationType.openaire3_0);
		case "openaire4.0":
			return Arrays.asList(ValidationType.openaire4_0, ValidationType.fair_literature_v4);
		case "openaire2.0_data":
			return Arrays.asList(ValidationType.fair_data);
		default:
			return new ArrayList<>();
		}

	}

	private static Iterator<OaAlertNotification> generateAlertNotifications(final ValidatorAlertMessage alertMessage,
			final Long date,
			final List<Subscription> subscriptions) {

		final OaAlertMappedFields fields = new OaAlertMappedFields();
		fields.setOriginalId(alertMessage.getOriginalId());
		fields.setTitle(alertMessage.getTitle());
		fields.setDatasourceId(alertMessage.getDatasourceId());
		fields.setDatasourceName(alertMessage.getDatasourceName());

		final String eventId = "evt-" + UUID.randomUUID();

		return subscriptions.stream().map(s -> {
			final OaAlertNotification n = new OaAlertNotification();
			n.setNotificationId("ntf-" + DigestUtils.md5Hex(s.getSubscriptionId() + "@@@" + eventId));
			n.setEventId(eventId);
			n.setDate(date);
			n.setMap(fields);

			try {
				n.setPayload(DHPUtils.MAPPER.writeValueAsString(alertMessage));
			} catch (final JsonProcessingException e) {
				n.setPayload(null);
			}
			n.setProducerId("OPENAIRE");
			n.setSubscriptionId(s.getSubscriptionId());
			n.setTopic(s.getTopic());
			return n;
		}).iterator();
	}

	private static ValidatorAlertMessage generatePayload(final String originalId,
			final String title,
			final String dsId,
			final String dsName,
			final StandardValidationResult standardValidationResult) {

		final ValidatorAlertMessage res = new ValidatorAlertMessage();
		res.setOriginalId(originalId);
		res.setTitle(title);
		res.setDatasourceId(dsId);
		res.setDatasourceName(dsName);
		res.setErrors(standardValidationResult
				.getResults()
				.entrySet()
				.stream()
				.map(e -> {
					final String field = e.getKey();
					return e
							.getValue()
							.getErrors()
							.stream()
							.map(err -> new ValidatorErrorMessage(field, err))
							.collect(Collectors.toList());
				})
				.flatMap(List::stream)
				.collect(Collectors.toList()));

		return res;

	}

	private static String extractDatasourceId(final Subscription sub) {
		return sub.conditionsAsList()
				.stream()
				.filter(c -> "datasourceId".equals(c.getField()))
				.map(MapCondition::getListParams)
				.filter(l -> !l.isEmpty())
				.map(l -> l.get(0).getValue())
				.findFirst()
				.orElse("");
	}
}
