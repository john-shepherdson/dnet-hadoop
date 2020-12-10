
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.TypedColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.stats.DatasourceStats;
import eu.dnetlib.dhp.broker.oa.util.aggregators.stats.StatsAggregator;

public class GenerateStatsJob {

	private static final Logger log = LoggerFactory.getLogger(GenerateStatsJob.class);

	public static void main(final String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(GenerateStatsJob.class
					.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/stats_params.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final SparkConf conf = new SparkConf();

		final String eventsPath = parser.get("outputDir") + "/events";
		log.info("eventsPath: {}", eventsPath);

		final String dbUrl = parser.get("dbUrl");
		log.info("dbUrl: {}", dbUrl);

		final String dbUser = parser.get("dbUser");
		log.info("dbUser: {}", dbUser);

		final String dbPassword = parser.get("dbPassword");
		log.info("dbPassword: {}", "***");

		final String brokerApiBaseUrl = parser.get("brokerApiBaseUrl");
		log.info("brokerApiBaseUrl: {}", brokerApiBaseUrl);

		final TypedColumn<Event, DatasourceStats> aggr = new StatsAggregator().toColumn();

		final Properties connectionProperties = new Properties();
		connectionProperties.put("user", dbUser);
		connectionProperties.put("password", dbPassword);

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils
				.readPath(spark, eventsPath, Event.class)
				.groupByKey(e -> e.getTopic() + "@@@" + e.getMap().getTargetDatasourceId(), Encoders.STRING())
				.agg(aggr)
				.map(t -> t._2, Encoders.bean(DatasourceStats.class))
				.write()
				.mode(SaveMode.Overwrite)
				.jdbc(dbUrl, "oa_datasource_stats_temp", connectionProperties);

			log.info("*** updateStats");
			updateStats(brokerApiBaseUrl);
			log.info("*** ALL done.");

		});
	}

	private static String updateStats(final String brokerApiBaseUrl) throws IOException {
		final String url = brokerApiBaseUrl + "/api/openaireBroker/stats/update";
		final HttpGet req = new HttpGet(url);

		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			try (final CloseableHttpResponse response = client.execute(req)) {
				return IOUtils.toString(response.getEntity().getContent());
			}
		}
	}

}
