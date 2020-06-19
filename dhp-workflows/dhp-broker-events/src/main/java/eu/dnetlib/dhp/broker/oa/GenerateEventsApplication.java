
package eu.dnetlib.dhp.broker.oa;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.broker.objects.OpenaireBrokerResult;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.EventFinder;
import eu.dnetlib.dhp.broker.oa.util.EventGroup;
import eu.dnetlib.dhp.broker.oa.util.aggregators.simple.ResultAggregator;
import eu.dnetlib.dhp.broker.oa.util.aggregators.simple.ResultGroup;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.OpenaireBrokerResultAggregator;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedDataset;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedProject;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedPublication;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedSoftware;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.config.DedupConfig;
import scala.Tuple2;

public class GenerateEventsApplication {

	private static final Logger log = LoggerFactory.getLogger(GenerateEventsApplication.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GenerateEventsApplication.class
						.getResourceAsStream("/eu/dnetlib/dhp/broker/oa/generate_broker_events.json")));
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

		final String isLookupUrl = parser.get("isLookupUrl");
		log.info("isLookupUrl: {}", isLookupUrl);

		final String dedupConfigProfileId = parser.get("dedupConfProfile");
		log.info("dedupConfigProfileId: {}", dedupConfigProfileId);

		final SparkConf conf = new SparkConf();
		// conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		// conf.registerKryoClasses(BrokerConstants.getModelClasses());

		// TODO UNCOMMENT
		// final DedupConfig dedupConfig = loadDedupConfig(isLookupUrl, dedupConfigProfileId);
		final DedupConfig dedupConfig = null;

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			removeOutputDir(spark, eventsPath);

			// TODO REMOVE THIS

			relatedProjects(spark, graphPath)
				.write()
				.mode(SaveMode.Overwrite)
				.json(eventsPath);

			// TODO UNCOMMENT THIS
			// spark
			// .emptyDataset(Encoders.bean(Event.class))
			// .union(generateEvents(spark, graphPath, Publication.class, dedupConfig))
			// .union(generateEvents(spark, graphPath, eu.dnetlib.dhp.schema.oaf.Dataset.class, dedupConfig))
			// .union(generateEvents(spark, graphPath, Software.class, dedupConfig))
			// .union(generateEvents(spark, graphPath, OtherResearchProduct.class, dedupConfig))
			// .write()
			// .mode(SaveMode.Overwrite)
			// .option("compression", "gzip")
			// .json(eventsPath);
		});

	}

	private static void removeOutputDir(final SparkSession spark, final String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	private static <SRC extends Result> Dataset<Event> generateEvents(
		final SparkSession spark,
		final String graphPath,
		final Class<SRC> sourceClass,
		final DedupConfig dedupConfig) {

		final Dataset<OpenaireBrokerResult> results = expandResultsWithRelations(spark, graphPath, sourceClass);

		final Dataset<Relation> mergedRels = readPath(spark, graphPath + "/relation", Relation.class)
			.filter(r -> r.getRelClass().equals(BrokerConstants.IS_MERGED_IN_CLASS));

		final TypedColumn<Tuple2<OpenaireBrokerResult, Relation>, ResultGroup> aggr = new ResultAggregator()
			.toColumn();

		return results
			.joinWith(mergedRels, results.col("openaireId").equalTo(mergedRels.col("source")), "inner")
			.groupByKey(
				(MapFunction<Tuple2<OpenaireBrokerResult, Relation>, String>) t -> t._2.getTarget(), Encoders.STRING())
			.agg(aggr)
			.map((MapFunction<Tuple2<String, ResultGroup>, ResultGroup>) t -> t._2, Encoders.bean(ResultGroup.class))
			.filter(rg -> rg.getData().size() > 1)
			.map(
				(MapFunction<ResultGroup, EventGroup>) g -> EventFinder.generateEvents(g, dedupConfig),
				Encoders.bean(EventGroup.class))
			.flatMap(group -> group.getData().iterator(), Encoders.bean(Event.class));
	}

	private static <SRC extends Result> Dataset<OpenaireBrokerResult> expandResultsWithRelations(
		final SparkSession spark,
		final String graphPath,
		final Class<SRC> sourceClass) {

		// final Dataset<eu.dnetlib.dhp.schema.oaf.Dataset> datasets = readPath(
		// spark, graphPath + "/dataset", eu.dnetlib.dhp.schema.oaf.Dataset.class);
		// final Dataset<Software> softwares = readPath(spark, graphPath + "/software", Software.class);
		// final Dataset<Publication> publications = readPath(spark, graphPath + "/publication", Publication.class);

		final Dataset<Relation> rels = readPath(spark, graphPath + "/relation", Relation.class)
			.filter(r -> !r.getRelClass().equals(BrokerConstants.IS_MERGED_IN_CLASS))
			.cache();

		final Dataset<OpenaireBrokerResult> r0 = readPath(
			spark, graphPath + "/" + sourceClass.getSimpleName().toLowerCase(), sourceClass)
				.filter(r -> r.getDataInfo().getDeletedbyinference())
				.map(ConversionUtils::oafResultToBrokerResult, Encoders.bean(OpenaireBrokerResult.class));

		// TODO UNCOMMENT THIS
		final Dataset<OpenaireBrokerResult> r1 = join(r0, rels, relatedProjects(spark, graphPath));
		// final Dataset<OpenaireBrokerResult> r2 = join(r1, rels, relatedDataset(spark, graphPath));
		// final Dataset<OpenaireBrokerResult> r3 = join(r2, rels, relatedPublications(spark, graphPath));
		// final Dataset<OpenaireBrokerResult> r4 = join(r3, rels, relatedSoftwares(spark, graphPath));

		return r1; // TODO it should be r4
	}

	private static Dataset<RelatedProject> relatedProjects(final SparkSession spark, final String graphPath) {

		final Dataset<Project> projects = readPath(spark, graphPath + "/project", Project.class);

		final Dataset<Relation> rels = readPath(spark, graphPath + "/relation", Relation.class)
			.filter(r -> r.getRelType().equals(ModelConstants.RESULT_PROJECT));

		return rels
			.joinWith(projects, projects.col("id").equalTo(rels.col("target")), "inner")
			.map(
				t -> new RelatedProject(
					t._1.getSource(),
					t._1.getRelType(),
					ConversionUtils.oafProjectToBrokerProject(t._2)),
				Encoders.bean(RelatedProject.class));
	}

	private static Dataset<RelatedDataset> relatedDataset(final SparkSession spark, final String graphPath) {

		final Dataset<eu.dnetlib.dhp.schema.oaf.Dataset> datasets = readPath(
			spark, graphPath + "/dataset", eu.dnetlib.dhp.schema.oaf.Dataset.class);

		final Dataset<Relation> rels = readPath(spark, graphPath + "/relation", Relation.class);

		return rels
			.joinWith(datasets, datasets.col("id").equalTo(rels.col("target")), "inner")
			.map(
				t -> new RelatedDataset(
					t._1.getSource(),
					t._1.getRelType(),
					ConversionUtils.oafDatasetToBrokerDataset(t._2)),
				Encoders.bean(RelatedDataset.class));
	}

	private static Dataset<RelatedSoftware> relatedSoftwares(final SparkSession spark, final String graphPath) {

		final Dataset<Software> softwares = readPath(spark, graphPath + "/software", Software.class);

		final Dataset<Relation> rels = readPath(spark, graphPath + "/relation", Relation.class);

		return rels
			.joinWith(softwares, softwares.col("id").equalTo(rels.col("target")), "inner")
			.map(
				t -> new RelatedSoftware(
					t._1.getSource(),
					t._1.getRelType(),
					ConversionUtils.oafSoftwareToBrokerSoftware(t._2)),
				Encoders.bean(RelatedSoftware.class));
	}

	private static Dataset<RelatedPublication> relatedPublications(final SparkSession spark, final String graphPath) {

		final Dataset<Publication> pubs = readPath(spark, graphPath + "/publication", Publication.class);

		final Dataset<Relation> rels = readPath(spark, graphPath + "/relation", Relation.class);

		return rels
			.joinWith(pubs, pubs.col("id").equalTo(rels.col("target")), "inner")
			.map(
				t -> new RelatedPublication(
					t._1.getSource(),
					t._1.getRelType(),
					ConversionUtils.oafPublicationToBrokerPublication(t._2)),
				Encoders.bean(RelatedPublication.class));
	}

	private static <T> Dataset<OpenaireBrokerResult> join(final Dataset<OpenaireBrokerResult> sources,
		final Dataset<Relation> rels,
		final Dataset<T> typedRels) {

		final TypedColumn<Tuple2<OpenaireBrokerResult, T>, OpenaireBrokerResult> aggr = new OpenaireBrokerResultAggregator<T>()
			.toColumn();

		return sources
			.joinWith(typedRels, sources.col("openaireId").equalTo(rels.col("source")), "left_outer")
			.groupByKey(
				(MapFunction<Tuple2<OpenaireBrokerResult, T>, String>) t -> t._1.getOpenaireId(), Encoders.STRING())
			.agg(aggr)
			.map(t -> t._2, Encoders.bean(OpenaireBrokerResult.class));

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
