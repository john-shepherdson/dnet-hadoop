
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.util.ClusterUtils;
import eu.dnetlib.dhp.broker.oa.util.EventFinder;
import eu.dnetlib.dhp.broker.oa.util.EventGroup;
import eu.dnetlib.dhp.broker.oa.util.aggregators.simple.ResultGroup;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;

public class GenerateEventsJob {

	private static final Logger log = LoggerFactory.getLogger(GenerateEventsJob.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GenerateEventsJob.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/generate_events.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String workingPath = parser.get("workingPath");
		log.info("workingPath: {}", workingPath);

		final String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		final String dedupConfigProfileId = parser.get("dedupConfProfile");
		log.info("dedupConfigProfileId: {}", dedupConfigProfileId);

		final String eventsPath = workingPath + "/events";
		log.info("eventsPath: {}", eventsPath);

		final SparkConf conf = new SparkConf();

		// TODO UNCOMMENT
		// final DedupConfig dedupConfig = loadDedupConfig(isLookupUrl, dedupConfigProfileId);
		final DedupConfig dedupConfig = null;

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			ClusterUtils.removeDir(spark, eventsPath);

			final Dataset<ResultGroup> groups = ClusterUtils
				.readPath(spark, workingPath + "/duplicates", ResultGroup.class);

			final Dataset<Event> events = groups
				.map(
					(MapFunction<ResultGroup, EventGroup>) g -> EventFinder.generateEvents(g, dedupConfig),
					Encoders.bean(EventGroup.class))
				.flatMap(group -> group.getData().iterator(), Encoders.bean(Event.class));

			events.write().mode(SaveMode.Overwrite).json(eventsPath);

		});

	}

	private static DedupConfig loadDedupConfig(final String isLookupUrl, final String profId) throws Exception {

		final ISLookUpService isLookUpService = ISLookupClientFactory.getLookUpService(isLookupUrl);

		final String conf = isLookUpService
			.getResourceProfileByQuery(
				String
					.format(
						"for $x in /RESOURCE_PROFILE[.//RESOURCE_IDENTIFIER/@value = '%s'] return $x//DEDUPLICATION/text()",
						profId));

		final DedupConfig dedupConfig = new ObjectMapper().readValue(conf, DedupConfig.class);
		dedupConfig.getPace().initModel();
		dedupConfig.getPace().initTranslationMap();
		// dedupConfig.getWf().setConfigurationId("???");

		return dedupConfig;
	}

}
