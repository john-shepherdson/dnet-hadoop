
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.model.EventFactory;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets.EnrichMissingDatasetIsReferencedBy;
import eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets.EnrichMissingDatasetIsRelatedTo;
import eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets.EnrichMissingDatasetIsSupplementedBy;
import eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets.EnrichMissingDatasetIsSupplementedTo;
import eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets.EnrichMissingDatasetReferences;
import eu.dnetlib.dhp.broker.oa.matchers.relatedProjects.EnrichMissingProject;
import eu.dnetlib.dhp.broker.oa.matchers.relatedProjects.EnrichMoreProject;
import eu.dnetlib.dhp.broker.oa.matchers.relatedPublications.EnrichMissingPublicationIsReferencedBy;
import eu.dnetlib.dhp.broker.oa.matchers.relatedPublications.EnrichMissingPublicationIsRelatedTo;
import eu.dnetlib.dhp.broker.oa.matchers.relatedPublications.EnrichMissingPublicationIsSupplementedBy;
import eu.dnetlib.dhp.broker.oa.matchers.relatedPublications.EnrichMissingPublicationIsSupplementedTo;
import eu.dnetlib.dhp.broker.oa.matchers.relatedPublications.EnrichMissingPublicationReferences;
import eu.dnetlib.dhp.broker.oa.matchers.relatedSoftware.EnrichMissingSoftware;
import eu.dnetlib.dhp.broker.oa.matchers.relatedSoftware.EnrichMoreSoftware;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMissingAbstract;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMissingAuthorOrcid;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMissingOpenAccess;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMissingPid;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMissingPublicationDate;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMissingSubject;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMoreOpenAccess;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMorePid;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMoreSubject;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;

public class GenerateEventsApplication {

	private static final Logger log = LoggerFactory.getLogger(GenerateEventsApplication.class);

	// Simple Matchers
	private static final UpdateMatcher<Result, ?> enrichMissingAbstract = new EnrichMissingAbstract();
	private static final UpdateMatcher<Result, ?> enrichMissingAuthorOrcid = new EnrichMissingAuthorOrcid();
	private static final UpdateMatcher<Result, ?> enrichMissingOpenAccess = new EnrichMissingOpenAccess();
	private static final UpdateMatcher<Result, ?> enrichMissingPid = new EnrichMissingPid();
	private static final UpdateMatcher<Result, ?> enrichMissingPublicationDate = new EnrichMissingPublicationDate();
	private static final UpdateMatcher<Result, ?> enrichMissingSubject = new EnrichMissingSubject();
	private static final UpdateMatcher<Result, ?> enrichMoreOpenAccess = new EnrichMoreOpenAccess();
	private static final UpdateMatcher<Result, ?> enrichMorePid = new EnrichMorePid();
	private static final UpdateMatcher<Result, ?> enrichMoreSubject = new EnrichMoreSubject();

	// Advanced matchers
	private static final UpdateMatcher<Pair<Result, List<Project>>, ?> enrichMissingProject = new EnrichMissingProject();
	private static final UpdateMatcher<Pair<Result, List<Project>>, ?> enrichMoreProject = new EnrichMoreProject();

	private static final UpdateMatcher<Pair<Result, List<Software>>, ?> enrichMissingSoftware = new EnrichMissingSoftware();
	private static final UpdateMatcher<Pair<Result, List<Software>>, ?> enrichMoreSoftware = new EnrichMoreSoftware();

	private static final UpdateMatcher<Pair<Result, List<Publication>>, ?> enrichMisissingPublicationIsRelatedTo = new EnrichMissingPublicationIsRelatedTo();
	private static final UpdateMatcher<Pair<Result, List<Publication>>, ?> enrichMissingPublicationIsReferencedBy =
		new EnrichMissingPublicationIsReferencedBy();
	private static final UpdateMatcher<Pair<Result, List<Publication>>, ?> enrichMissingPublicationReferences = new EnrichMissingPublicationReferences();
	private static final UpdateMatcher<Pair<Result, List<Publication>>, ?> enrichMissingPublicationIsSupplementedTo =
		new EnrichMissingPublicationIsSupplementedTo();
	private static final UpdateMatcher<Pair<Result, List<Publication>>, ?> enrichMissingPublicationIsSupplementedBy =
		new EnrichMissingPublicationIsSupplementedBy();

	private static final UpdateMatcher<Pair<Result, List<eu.dnetlib.dhp.schema.oaf.Dataset>>, ?> enrichMisissingDatasetIsRelatedTo =
		new EnrichMissingDatasetIsRelatedTo();
	private static final UpdateMatcher<Pair<Result, List<eu.dnetlib.dhp.schema.oaf.Dataset>>, ?> enrichMissingDatasetIsReferencedBy =
		new EnrichMissingDatasetIsReferencedBy();
	private static final UpdateMatcher<Pair<Result, List<eu.dnetlib.dhp.schema.oaf.Dataset>>, ?> enrichMissingDatasetReferences =
		new EnrichMissingDatasetReferences();
	private static final UpdateMatcher<Pair<Result, List<eu.dnetlib.dhp.schema.oaf.Dataset>>, ?> enrichMissingDatasetIsSupplementedTo =
		new EnrichMissingDatasetIsSupplementedTo();
	private static final UpdateMatcher<Pair<Result, List<eu.dnetlib.dhp.schema.oaf.Dataset>>, ?> enrichMissingDatasetIsSupplementedBy =
		new EnrichMissingDatasetIsSupplementedBy();

	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(GenerateEventsApplication.class
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
			final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
			removeOutputDir(spark, eventsPath);

			final JavaRDD<Event> eventsRdd = sc.emptyRDD();

			for (final Class<? extends Result> r1 : BrokerConstants.RESULT_CLASSES) {
				eventsRdd.union(generateSimpleEvents(spark, graphPath, r1));

				for (final Class<? extends Result> r2 : BrokerConstants.RESULT_CLASSES) {
					eventsRdd.union(generateRelationEvents(spark, graphPath, r1, r2));
				}
			}

			eventsRdd.saveAsTextFile(eventsPath, GzipCodec.class);
		});

	}

	private static void removeOutputDir(final SparkSession spark, final String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	private static <R extends Result> JavaRDD<Event> generateSimpleEvents(final SparkSession spark,
		final String graphPath,
		final Class<R> resultClazz) {

		final Dataset<R> results = readPath(spark, graphPath + "/" + resultClazz.getSimpleName().toLowerCase(), resultClazz)
			.filter(r -> r.getDataInfo().getDeletedbyinference());

		final Dataset<Relation> rels = readPath(spark, graphPath + "/relation", Relation.class)
			.filter(r -> r.getRelClass().equals(BrokerConstants.IS_MERGED_IN_CLASS));

		final Column c = null; // TODO

		final Dataset<Row> aa = results
			.joinWith(rels, results.col("id").equalTo(rels.col("source")), "inner")
			.groupBy(rels.col("target"))
			.agg(c)
			.filter(x -> x.size() > 1)
		// generateSimpleEvents(...)
		// flatMap()
		// toRdd()
		;

		return null;

	}

	private List<Event> generateSimpleEvents(final Collection<Result> children) {
		final List<UpdateInfo<?>> list = new ArrayList<>();

		for (final Result target : children) {
			list.addAll(enrichMissingAbstract.searchUpdatesForRecord(target, children));
			list.addAll(enrichMissingAuthorOrcid.searchUpdatesForRecord(target, children));
			list.addAll(enrichMissingOpenAccess.searchUpdatesForRecord(target, children));
			list.addAll(enrichMissingPid.searchUpdatesForRecord(target, children));
			list.addAll(enrichMissingPublicationDate.searchUpdatesForRecord(target, children));
			list.addAll(enrichMissingSubject.searchUpdatesForRecord(target, children));
			list.addAll(enrichMoreOpenAccess.searchUpdatesForRecord(target, children));
			list.addAll(enrichMorePid.searchUpdatesForRecord(target, children));
			list.addAll(enrichMoreSubject.searchUpdatesForRecord(target, children));
		}

		return list.stream().map(EventFactory::newBrokerEvent).collect(Collectors.toList());
	}

	private static <SRC extends Result, TRG extends OafEntity> JavaRDD<Event> generateRelationEvents(final SparkSession spark,
		final String graphPath,
		final Class<SRC> sourceClass,
		final Class<TRG> targetClass) {

		final Dataset<SRC> sources = readPath(spark, graphPath + "/" + sourceClass.getSimpleName().toLowerCase(), sourceClass)
			.filter(r -> r.getDataInfo().getDeletedbyinference());

		final Dataset<TRG> targets = readPath(spark, graphPath + "/" + sourceClass.getSimpleName().toLowerCase(), targetClass);

		final Dataset<Relation> mergedRels = readPath(spark, graphPath + "/relation", Relation.class)
			.filter(r -> r.getRelClass().equals(BrokerConstants.IS_MERGED_IN_CLASS));

		final Dataset<Relation> rels = readPath(spark, graphPath + "/relation", Relation.class)
			.filter(r -> !r.getRelClass().equals(BrokerConstants.IS_MERGED_IN_CLASS));

		if (targetClass == Project.class) {
			// TODO join using: generateProjectsEvents
		} else if (targetClass == Software.class) {
			// TODO join using: generateSoftwareEvents
		} else if (targetClass == Publication.class) {
			// TODO join using: generatePublicationRelatedEvents
		} else if (targetClass == eu.dnetlib.dhp.schema.oaf.Dataset.class) {
			// TODO join using: generateDatasetRelatedEvents
		}

		return null;
	}

	private List<Event> generateProjectsEvents(final Collection<Pair<Result, List<Project>>> childrenWithProjects) {
		final List<UpdateInfo<?>> list = new ArrayList<>();

		for (final Pair<Result, List<Project>> target : childrenWithProjects) {
			list.addAll(enrichMissingProject.searchUpdatesForRecord(target, childrenWithProjects));
			list.addAll(enrichMoreProject.searchUpdatesForRecord(target, childrenWithProjects));
		}

		return list.stream().map(EventFactory::newBrokerEvent).collect(Collectors.toList());
	}

	private List<Event> generateSoftwareEvents(final Collection<Pair<Result, List<Software>>> childrenWithSoftwares) {
		final List<UpdateInfo<?>> list = new ArrayList<>();

		for (final Pair<Result, List<Software>> target : childrenWithSoftwares) {
			list.addAll(enrichMissingSoftware.searchUpdatesForRecord(target, childrenWithSoftwares));
			list.addAll(enrichMoreSoftware.searchUpdatesForRecord(target, childrenWithSoftwares));
		}
		return list.stream().map(EventFactory::newBrokerEvent).collect(Collectors.toList());
	}

	private List<Event> generatePublicationRelatedEvents(final String relType,
		final Collection<Pair<Result, Map<String, List<Publication>>>> childrenWithRels) {

		final List<UpdateInfo<?>> list = new ArrayList<>();

		final List<Pair<Result, List<Publication>>> cleanedChildrens = childrenWithRels
			.stream()
			.filter(p -> p.getRight().containsKey(relType))
			.map(p -> Pair.of(p.getLeft(), p.getRight().get(relType)))
			.filter(p -> p.getRight().size() > 0)
			.collect(Collectors.toList());

		for (final Pair<Result, List<Publication>> target : cleanedChildrens) {
			if (relType.equals("isRelatedTo")) {
				list.addAll(enrichMisissingPublicationIsRelatedTo.searchUpdatesForRecord(target, cleanedChildrens));
			} else if (relType.equals("references")) {
				list.addAll(enrichMissingPublicationReferences.searchUpdatesForRecord(target, cleanedChildrens));
			} else if (relType.equals("isReferencedBy")) {
				list.addAll(enrichMissingPublicationIsReferencedBy.searchUpdatesForRecord(target, cleanedChildrens));
			} else if (relType.equals("isSupplementedTo")) {
				list.addAll(enrichMissingPublicationIsSupplementedTo.searchUpdatesForRecord(target, cleanedChildrens));
			} else if (relType.equals("isSupplementedBy")) {
				list.addAll(enrichMissingPublicationIsSupplementedBy.searchUpdatesForRecord(target, cleanedChildrens));
			}
		}

		return list.stream().map(EventFactory::newBrokerEvent).collect(Collectors.toList());

	}

	private List<Event> generateDatasetRelatedEvents(final String relType,
		final Collection<Pair<Result, Map<String, List<eu.dnetlib.dhp.schema.oaf.Dataset>>>> childrenWithRels) {

		final List<UpdateInfo<?>> list = new ArrayList<>();

		final List<Pair<Result, List<eu.dnetlib.dhp.schema.oaf.Dataset>>> cleanedChildrens = childrenWithRels
			.stream()
			.filter(p -> p.getRight().containsKey(relType))
			.map(p -> Pair.of(p.getLeft(), p.getRight().get(relType)))
			.filter(p -> p.getRight().size() > 0)
			.collect(Collectors.toList());

		for (final Pair<Result, List<eu.dnetlib.dhp.schema.oaf.Dataset>> target : cleanedChildrens) {
			if (relType.equals("isRelatedTo")) {
				list.addAll(enrichMisissingDatasetIsRelatedTo.searchUpdatesForRecord(target, cleanedChildrens));
			} else if (relType.equals("references")) {
				list.addAll(enrichMissingDatasetReferences.searchUpdatesForRecord(target, cleanedChildrens));
			} else if (relType.equals("isReferencedBy")) {
				list.addAll(enrichMissingDatasetIsReferencedBy.searchUpdatesForRecord(target, cleanedChildrens));
			} else if (relType.equals("isSupplementedTo")) {
				list.addAll(enrichMissingDatasetIsSupplementedTo.searchUpdatesForRecord(target, cleanedChildrens));
			} else if (relType.equals("isSupplementedBy")) {
				list.addAll(enrichMissingDatasetIsSupplementedBy.searchUpdatesForRecord(target, cleanedChildrens));
			}
		}

		return list.stream().map(EventFactory::newBrokerEvent).collect(Collectors.toList());

	}

	public static <R> Dataset<R> readPath(
		final SparkSession spark,
		final String inputPath,
		final Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}
}
