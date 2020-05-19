
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

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.model.EventFactory;
import eu.dnetlib.dhp.broker.oa.matchers.EnrichMissingAbstract;
import eu.dnetlib.dhp.broker.oa.matchers.EnrichMissingAuthorOrcid;
import eu.dnetlib.dhp.broker.oa.matchers.EnrichMissingOpenAccess;
import eu.dnetlib.dhp.broker.oa.matchers.EnrichMissingPid;
import eu.dnetlib.dhp.broker.oa.matchers.EnrichMissingProject;
import eu.dnetlib.dhp.broker.oa.matchers.EnrichMissingPublicationDate;
import eu.dnetlib.dhp.broker.oa.matchers.EnrichMissingSubject;
import eu.dnetlib.dhp.broker.oa.matchers.EnrichMoreOpenAccess;
import eu.dnetlib.dhp.broker.oa.matchers.EnrichMorePid;
import eu.dnetlib.dhp.broker.oa.matchers.EnrichMoreSubject;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.oaf.Result;

public class GenerateEventsApplication {

	private static final Logger log = LoggerFactory.getLogger(GenerateEventsApplication.class);

	private static final UpdateMatcher<?> enrichMissingAbstract = new EnrichMissingAbstract();
	private static final UpdateMatcher<?> enrichMissingAuthorOrcid = new EnrichMissingAuthorOrcid();
	private static final UpdateMatcher<?> enrichMissingOpenAccess = new EnrichMissingOpenAccess();
	private static final UpdateMatcher<?> enrichMissingPid = new EnrichMissingPid();
	private static final UpdateMatcher<?> enrichMissingProject = new EnrichMissingProject();
	private static final UpdateMatcher<?> enrichMissingPublicationDate = new EnrichMissingPublicationDate();
	private static final UpdateMatcher<?> enrichMissingSubject = new EnrichMissingSubject();
	private static final UpdateMatcher<?> enrichMoreOpenAccess = new EnrichMoreOpenAccess();
	private static final UpdateMatcher<?> enrichMorePid = new EnrichMorePid();
	private static final UpdateMatcher<?> enrichMoreSubject = new EnrichMoreSubject();

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
		final List<UpdateInfo<?>> list = new ArrayList<>();

		for (final Result target : children) {
			list.addAll(enrichMissingAbstract.searchUpdatesForRecord(target, children));
			list.addAll(enrichMissingAuthorOrcid.searchUpdatesForRecord(target, children));
			list.addAll(enrichMissingOpenAccess.searchUpdatesForRecord(target, children));
			list.addAll(enrichMissingPid.searchUpdatesForRecord(target, children));
			list.addAll(enrichMissingProject.searchUpdatesForRecord(target, children));
			list.addAll(enrichMissingPublicationDate.searchUpdatesForRecord(target, children));
			list.addAll(enrichMissingSubject.searchUpdatesForRecord(target, children));
			list.addAll(enrichMoreOpenAccess.searchUpdatesForRecord(target, children));
			list.addAll(enrichMorePid.searchUpdatesForRecord(target, children));
			list.addAll(enrichMoreSubject.searchUpdatesForRecord(target, children));
		}

		return list.stream().map(EventFactory::newBrokerEvent).collect(Collectors.toList());
	}

}
