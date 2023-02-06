
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.HashSet;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.provision.model.ProvisionModelSupport;
import eu.dnetlib.dhp.oa.provision.model.SortableRelationKey;
import eu.dnetlib.dhp.oa.provision.utils.RelationPartitioner;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

/**
 * PrepareRelationsJob prunes the relationships: only consider relationships that are not virtually deleted
 * ($.dataInfo.deletedbyinference == false), each entity can be linked at most to 100 other objects
 */
public class PrepareRelationsJob {

	private static final Logger log = LoggerFactory.getLogger(PrepareRelationsJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final int MAX_RELS = 100;

	public static final int DEFAULT_NUM_PARTITIONS = 3000;

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				PrepareRelationsJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/provision/input_params_prepare_relations.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputRelationsPath = parser.get("inputRelationsPath");
		log.info("inputRelationsPath: {}", inputRelationsPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		int relPartitions = Optional
			.ofNullable(parser.get("relPartitions"))
			.map(Integer::valueOf)
			.orElse(DEFAULT_NUM_PARTITIONS);
		log.info("relPartitions: {}", relPartitions);

		Set<String> relationFilter = Optional
			.ofNullable(parser.get("relationFilter"))
			.map(String::toLowerCase)
			.map(s -> Sets.newHashSet(Splitter.on(",").split(s)))
			.orElse(new HashSet<>());
		log.info("relationFilter: {}", relationFilter);

		int sourceMaxRelations = Optional
			.ofNullable(parser.get("sourceMaxRelations"))
			.map(Integer::valueOf)
			.orElse(MAX_RELS);
		log.info("sourceMaxRelations: {}", sourceMaxRelations);

		int targetMaxRelations = Optional
			.ofNullable(parser.get("targetMaxRelations"))
			.map(Integer::valueOf)
			.orElse(MAX_RELS);
		log.info("targetMaxRelations: {}", targetMaxRelations);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ProvisionModelSupport.getModelClasses());

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareRelationsRDD(
					spark, inputRelationsPath, outputPath, relationFilter, sourceMaxRelations, targetMaxRelations,
					relPartitions);
			});
	}

	/**
	 * RDD based implementation that prepares the graph relations by limiting the number of outgoing links and filtering
	 * the relation types according to the given criteria. Moreover, outgoing links kept within the given limit are
	 * prioritized according to the weights indicated in eu.dnetlib.dhp.oa.provision.model.SortableRelation.
	 *
	 * @param spark the spark session
	 * @param inputRelationsPath source path for the graph relations
	 * @param outputPath output path for the processed relations
	 * @param relationFilter set of relation filters applied to the `relClass` field
	 * @param sourceMaxRelations maximum number of allowed outgoing edges grouping by relation.source
	 * @param targetMaxRelations maximum number of allowed outgoing edges grouping by relation.target
	 * @param relPartitions number of partitions for the output RDD
	 */
	private static void prepareRelationsRDD(SparkSession spark, String inputRelationsPath, String outputPath,
		Set<String> relationFilter, int sourceMaxRelations, int targetMaxRelations, int relPartitions) {

		JavaRDD<Relation> rels = readPathRelationRDD(spark, inputRelationsPath)
			.filter(rel -> !(rel.getSource().startsWith("unresolved") || rel.getTarget().startsWith("unresolved")))
			.filter(rel -> !relationFilter.contains(StringUtils.lowerCase(rel.getRelClass())));

		JavaRDD<Relation> pruned = pruneRels(
			pruneRels(
				rels,
				sourceMaxRelations, relPartitions, (Function<Relation, String>) Relation::getSource),
			targetMaxRelations, relPartitions, (Function<Relation, String>) Relation::getTarget);
		spark
			.createDataset(pruned.rdd(), Encoders.bean(Relation.class))
			.repartition(relPartitions)
			.write()
			.mode(SaveMode.Overwrite)
			.parquet(outputPath);
	}

	private static JavaRDD<Relation> pruneRels(JavaRDD<Relation> rels, int maxRelations,
		int relPartitions, Function<Relation, String> idFn) {
		return rels
			.mapToPair(r -> new Tuple2<>(SortableRelationKey.create(r, idFn.call(r)), r))
			.repartitionAndSortWithinPartitions(new RelationPartitioner(relPartitions))
			.groupBy(Tuple2::_1)
			.map(Tuple2::_2)
			.map(t -> Iterables.limit(t, maxRelations))
			.flatMap(Iterable::iterator)
			.map(Tuple2::_2);
	}

	// experimental
	private static void prepareRelationsDataset(
		SparkSession spark, String inputRelationsPath, String outputPath, Set<String> relationFilter, int maxRelations,
		int relPartitions) {
		spark
			.read()
			.textFile(inputRelationsPath)
			.repartition(relPartitions)
			.map(
				(MapFunction<String, Relation>) s -> OBJECT_MAPPER.readValue(s, Relation.class),
				Encoders.kryo(Relation.class))
			.filter((FilterFunction<Relation>) rel -> !relationFilter.contains(rel.getRelClass()))
			.groupByKey(
				(MapFunction<Relation, String>) Relation::getSource,
				Encoders.STRING())
			.agg(new RelationAggregator(maxRelations).toColumn())
			.flatMap(
				(FlatMapFunction<Tuple2<String, RelationList>, Relation>) t -> Iterables
					.limit(t._2().getRelations(), maxRelations)
					.iterator(),
				Encoders.bean(Relation.class))
			.repartition(relPartitions)
			.write()
			.mode(SaveMode.Overwrite)
			.parquet(outputPath);
	}

	public static class RelationAggregator
		extends Aggregator<Relation, RelationList, RelationList> {

		private final int maxRelations;

		public RelationAggregator(int maxRelations) {
			this.maxRelations = maxRelations;
		}

		@Override
		public RelationList zero() {
			return new RelationList();
		}

		@Override
		public RelationList reduce(RelationList b, Relation a) {
			b.getRelations().add(a);
			return getSortableRelationList(b);
		}

		@Override
		public RelationList merge(RelationList b1, RelationList b2) {
			b1.getRelations().addAll(b2.getRelations());
			return getSortableRelationList(b1);
		}

		@Override
		public RelationList finish(RelationList r) {
			return getSortableRelationList(r);
		}

		private RelationList getSortableRelationList(RelationList b1) {
			RelationList sr = new RelationList();
			sr
				.setRelations(
					b1
						.getRelations()
						.stream()
						.limit(maxRelations)
						.collect(Collectors.toCollection(() -> new PriorityQueue<>(new RelationComparator()))));
			return sr;
		}

		@Override
		public Encoder<RelationList> bufferEncoder() {
			return Encoders.kryo(RelationList.class);
		}

		@Override
		public Encoder<RelationList> outputEncoder() {
			return Encoders.kryo(RelationList.class);
		}
	}

	/**
	 * Reads a JavaRDD of eu.dnetlib.dhp.oa.provision.model.SortableRelation objects from a newline delimited json text
	 * file,
	 *
	 * @param spark
	 * @param inputPath
	 * @return the JavaRDD<SortableRelation> containing all the relationships
	 */
	private static JavaRDD<Relation> readPathRelationRDD(
		SparkSession spark, final String inputPath) {
		JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		return sc.textFile(inputPath).map(s -> OBJECT_MAPPER.readValue(s, Relation.class));
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}
}
