
package eu.dnetlib.dhp.broker.oa_alerts;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

import eu.dnetlib.broker.objects.alerts.ValidatorAlertMessage;
import eu.dnetlib.broker.objects.alerts.ValidatorErrorMessage;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.MappedFields;
import eu.dnetlib.dhp.broker.model.Notification;
import eu.dnetlib.dhp.broker.model.Subscription;
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

		final String outputPath = parser.get("outputDir");

		final String dsId = DHPUtils.MAPPER.readValue(parser.get("provenance"), Provenance.class).getDatasourceId();
		log.info("dsId: {}", dsId);

		final String dsName = DHPUtils.MAPPER.readValue(parser.get("provenance"), Provenance.class).getDatasourceName();
		log.info("dsName: {}", dsName);

		final String compatibilityLevel = DHPUtils.MAPPER.readValue(parser.get("apidescriptor"), ApiDescriptor.class).getCompatibilityLevel();
		log.info("compatibilityLevel: {}", compatibilityLevel);

		final MDStoreVersion mdstoreVersion = DHPUtils.MAPPER.readValue(mdstoreInputVersion, MDStoreVersion.class);
		final String inputPath = mdstoreVersion.getHdfsPath() + Constants.MDSTORE_DATA_PATH;
		log.info("inputPath: {}", inputPath);

		final String brokerApiBaseUrl = parser.get("brokerApiBaseUrl");
		log.info("brokerApiBaseUrl: {}", brokerApiBaseUrl);

		if (StringUtils.isAnyBlank(dsId, compatibilityLevel, inputPath, brokerApiBaseUrl)) { throw new RuntimeException("A required information is missing"); }

		ValidationType validationType;
		switch (compatibilityLevel) {
		case "openaire2.0":
			validationType = ValidationType.openaire2_0;
			break;
		case "openaire3.0":
			validationType = ValidationType.openaire3_0;
			break;
		case "openaire4.0":
			validationType = ValidationType.openaire4_0;
			break;
		case "fair_data":
			validationType = ValidationType.fair_data;
			break;
		case "fair_literature_v4":
			validationType = ValidationType.fair_literature_v4;
			break;
		default:
			validationType = null;
			break;
		}

		if (validationType == null) {
			log.warn("The compatibility is non managed by the validator engine");
			return;
		}

		final String topic = "ALERT/" + validationType;

		log.info("topic: {}", topic);

		final SparkConf conf = new SparkConf();

		SparkSessionSupport.runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			final LongAccumulator total = spark.sparkContext().longAccumulator("total_alert_notifications");

			final Dataset<ValidatorAlertMessage> payloads = spark
					.read()
					.parquet(inputPath)
					.as(Encoders.bean(MetadataRecord.class))
					.filter(r -> r.getValidationResults().containsKey(validationType))
					.map(r -> generatePayload(r.getOriginalId(), dsId, dsName, r.getValidationResults().get(validationType)), Encoders
							.bean(ValidatorAlertMessage.class));

			final DatasourceStats stats = new DatasourceStats();
			stats.setId(dsId);
			stats.setName(dsName);
			stats.setType("-"); // TODO
			stats.setTopic(topic);
			stats.setSize(payloads.count());

			updateStats(brokerApiBaseUrl, stats);

			final List<Subscription> subscriptions = listSubscriptions(dsId, topic);

			final Long date = new Date().getTime();
			log.info("date: {}", date);

			if (subscriptions.size() > 0) {

				final Dataset<Notification> dataset =
						payloads.flatMap(p -> generateAlertNotifications(p, date, subscriptions).iterator(), Encoders.bean(Notification.class))
								.filter(n -> StringUtils.isNotBlank(n.getPayload()));

				ClusterUtils.save(dataset, outputPath, Notification.class, total);
			} else {
				ClusterUtils.save(spark.emptyDataset(Encoders.bean(Notification.class)), outputPath, Notification.class, total);
			}
		});
	}

	private static List<Notification> generateAlertNotifications(final ValidatorAlertMessage alertMessage,
			final Long date,
			final List<Subscription> subscriptions) {

		final MappedFields fields = new MappedFields();

		// TODO correggere il modello
		// fields.put("originalId", alertMessage.getOriginalId());
		// fields.put("datasourceId", alertMessage.getDatasourceId());
		// fields.put("datasourceName", alertMessage.getDatasourceName());

		final String eventId = "evt-" + UUID.randomUUID();

		return subscriptions.stream().map(s -> {
			final Notification n = new Notification();
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
		}).collect(Collectors.toList());
	}

	private static List<Subscription> listSubscriptions(final String dsId, final String topic) {
		// TODO Auto-generated method stub
		return null;
	}

	private static void updateStats(final String brokerApiBaseUrl, final DatasourceStats stats) throws IOException {

		final HttpPost req = new HttpPost(brokerApiBaseUrl + "/api/openaire-alerts/stats/update");
		req.setHeader("Accept", "application/json");
		req.setHeader("Content-type", "application/json");
		req.setEntity(new StringEntity(DHPUtils.MAPPER.writeValueAsString(stats)));

		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			try (final CloseableHttpResponse response = client.execute(req)) {

			}
		}
	}

	private static ValidatorAlertMessage generatePayload(final String originalId,
			final String dsId,
			final String dsName,
			final StandardValidationResult standardValidationResult) {

		final ValidatorAlertMessage res = new ValidatorAlertMessage();
		res.setOriginalId(originalId);
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

}
