
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.model.EventFactory;
import eu.dnetlib.dhp.broker.oa.util.EnrichMissingAbstract;
import eu.dnetlib.dhp.broker.oa.util.EnrichMissingAuthorOrcid;
import eu.dnetlib.dhp.broker.oa.util.EnrichMissingOpenAccess;
import eu.dnetlib.dhp.broker.oa.util.EnrichMissingPid;
import eu.dnetlib.dhp.broker.oa.util.EnrichMissingProject;
import eu.dnetlib.dhp.broker.oa.util.EnrichMissingPublicationDate;
import eu.dnetlib.dhp.broker.oa.util.EnrichMissingSubject;
import eu.dnetlib.dhp.broker.oa.util.EnrichMoreOpenAccess;
import eu.dnetlib.dhp.broker.oa.util.EnrichMorePid;
import eu.dnetlib.dhp.broker.oa.util.EnrichMoreSubject;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.oaf.Result;

public class GenerateEventsApplication {

	private static final Logger log = LoggerFactory.getLogger(GenerateEventsApplication.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GenerateEventsApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/merge_claims_parameters.json")));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String graphPath = parser.get("graphPath");
		log.info("graphPath: {}", graphPath);

		final String eventsPath = parser.get("eventsPath");
		log.info("eventsPath: {}", eventsPath);

		final SparkConf conf = new SparkConf();
		runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			removeOutputDir(spark, eventsPath);
			generateEvents(spark, graphPath, eventsPath);
		});

	}

	private static void removeOutputDir(final SparkSession spark, final String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	private static void generateEvents(final SparkSession spark, final String graphPath, final String eventsPath) {
		// TODO
	}

	private List<Event> generateEvents(final Result... children) {
		final List<Event> list = new ArrayList<>();

		for (final Result source : children) {
			for (final Result target : children) {
				if (source != target) {
					list
						.addAll(
							findUpdates(source, target)
								.stream()
								.map(info -> EventFactory.newBrokerEvent(source, target, info))
								.collect(Collectors.toList()));
				}
			}
		}

		return list;
	}

	private List<UpdateInfo<?>> findUpdates(final Result source, final Result target) {
		final List<UpdateInfo<?>> list = new ArrayList<>();
		list.addAll(EnrichMissingAbstract.findUpdates(source, target));
		list.addAll(EnrichMissingAuthorOrcid.findUpdates(source, target));
		list.addAll(EnrichMissingOpenAccess.findUpdates(source, target));
		list.addAll(EnrichMissingPid.findUpdates(source, target));
		list.addAll(EnrichMissingProject.findUpdates(source, target));
		list.addAll(EnrichMissingPublicationDate.findUpdates(source, target));
		list.addAll(EnrichMissingSubject.findUpdates(source, target));
		list.addAll(EnrichMoreOpenAccess.findUpdates(source, target));
		list.addAll(EnrichMorePid.findUpdates(source, target));
		list.addAll(EnrichMoreSubject.findUpdates(source, target));
		return list;
	}

}
