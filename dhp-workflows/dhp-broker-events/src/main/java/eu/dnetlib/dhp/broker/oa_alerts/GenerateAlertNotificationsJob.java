package eu.dnetlib.dhp.broker.oa_alerts;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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
import eu.dnetlib.dhp.broker.model.Notification;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.common.Constants;
import eu.dnetlib.dhp.common.SparkSessionSupport;
import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion;
import eu.dnetlib.dhp.schema.mdstore.MetadataRecord;
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

		final MDStoreVersion mdstoreVersion = DHPUtils.MAPPER.readValue(mdstoreInputVersion, MDStoreVersion.class);
		final String inputPath = mdstoreVersion.getHdfsPath() + Constants.MDSTORE_DATA_PATH;

		log.info("inputPath: {}", inputPath);

		final Long date = new Date().getTime();

		final SparkConf conf = new SparkConf();

		SparkSessionSupport.runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			final LongAccumulator total = spark.sparkContext().longAccumulator("total_alert_notifications");

			// TODO: Calcolare il totale per ogni topic e aggiornare il database del broker usando l'api
			// TODO: Incrociare le potenziali notifiche con le subscription
			// TODO: Indicizzare

			final Dataset<Notification> dataset = spark.read()
					.parquet(inputPath)
					.as(Encoders.bean(MetadataRecord.class))
					.flatMap(r -> generateAlertNotifications(r, date), Encoders.bean(Notification.class));

			ClusterUtils.save(dataset, outputPath, Notification.class, total);
		});
	}

	private static Iterator<Notification> generateAlertNotifications(final MetadataRecord r, final Long date) {
		if ((r == null)
				|| (r.getProvenance() == null)
				|| StringUtils.isAnyBlank(r.getOriginalId(), r.getProvenance().getDatasourceId(), r.getProvenance().getDatasourceName())) {
			return new ArrayList<Notification>().iterator();
		}

		final String originalId = r.getOriginalId();
		final String dsId = r.getProvenance().getDatasourceId();
		final String dsName = r.getProvenance().getDatasourceName();

		return r.getValidationResults()
				.entrySet()
				.stream()
				.map(e -> {
					// TODO
					final Notification n = new Notification();
					n.setNotificationId(null);
					n.setEventId(null);
					n.setDate(date);
					n.setMap(null);
					n.setPayload(generatePayload(originalId, dsId, dsName, e.getValue()));
					n.setProducerId(null);
					n.setSubscriptionId(null);
					n.setTopic(null);
					return n;
				})
				.filter(n -> StringUtils.isNotBlank(n.getPayload()))
				.iterator();

	}

	private static String generatePayload(final String originalId,
			final String dsId,
			final String dsName,
			final StandardValidationResult standardValidationResult) {

		final ValidatorAlertMessage res = new ValidatorAlertMessage();
		res.setOriginalId(originalId);
		res.setDatasourceId(dsId);
		res.setDatasourceName(dsName);
		res.setErrors(standardValidationResult.getResults()
				.entrySet()
				.stream()
				.map(e -> {
					final String field = e.getKey();
					return e.getValue()
							.getErrors()
							.stream()
							.map(err -> new ValidatorErrorMessage(field, err))
							.collect(Collectors.toList());
				})
				.flatMap(List::stream)
				.collect(Collectors.toList()));

		try {
			return DHPUtils.MAPPER.writeValueAsString(standardValidationResult);
		} catch (final JsonProcessingException e1) {
			log.warn("Error serializing payload");
			return null;
		}

	}

}
