
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.provision.model.ProvisionModelSupport;
import eu.dnetlib.dhp.oa.provision.model.SortableRelationKey;
import eu.dnetlib.dhp.oa.provision.utils.RelationPartitioner;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

/**
 * Joins the graph nodes by resolving the links of distance = 1 to create an adjacency list of linked objects. The
 * operation considers all the entity types (publication, dataset, software, ORP, project, datasource, organization, and
 * all the possible relationships (similarity links produced by the Dedup process are excluded).
 * <p>
 * The operation is implemented by sequentially joining one entity type at time (E) with the relationships (R), and
 * again by E, finally grouped by E.id;
 * <p>
 * The workflow is organized in different parts aimed to to reduce the complexity of the operation 1)
 * PrepareRelationsJob: only consider relationships that are not virtually deleted ($.dataInfo.deletedbyinference ==
 * false), each entity can be linked at most to 100 other objects
 * <p>
 * 2) JoinRelationEntityByTargetJob: (phase 1): prepare tuples [relation - target entity] (R - T): for each entity type
 * E_i map E_i as RelatedEntity T_i to simplify the model and extracting only the necessary information join (R.target =
 * T_i.id) save the tuples (R_i, T_i) (phase 2): create the union of all the entity types E, hash by id read the tuples
 * (R, T), hash by R.source join E.id = (R, T).source, where E becomes the Source Entity S save the tuples (S, R, T)
 * <p>
 * 3) AdjacencyListBuilderJob: given the tuple (S - R - T) we need to group by S.id -> List [ R - T ], mapping the
 * result as JoinedEntity
 * <p>
 * 4) XmlConverterJob: convert the JoinedEntities as XML records
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
			.map(s -> Sets.newHashSet(Splitter.on(",").split(s)))
			.orElse(new HashSet<>());
		log.info("relationFilter: {}", relationFilter);

		int maxRelations = Optional
			.ofNullable(parser.get("maxRelations"))
			.map(Integer::valueOf)
			.orElse(MAX_RELS);
		log.info("maxRelations: {}", maxRelations);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ProvisionModelSupport.getModelClasses());

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareRelationsDataset(
					spark, inputRelationsPath, outputPath, relationFilter, maxRelations, relPartitions);
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
	 * @param maxRelations maximum number of allowed outgoing edges
	 * @param relPartitions number of partitions for the output RDD
	 */
	private static void prepareRelationsRDD(SparkSession spark, String inputRelationsPath, String outputPath,
		Set<String> relationFilter, int maxRelations, int relPartitions) {

		// group by SOURCE and apply limit
		RDD<Relation> bySource = readPathRelationRDD(spark, inputRelationsPath)
			.filter(rel -> rel.getDataInfo().getDeletedbyinference() == false)
			.filter(rel -> relationFilter.contains(rel.getRelClass()) == false)
			.mapToPair(r -> new Tuple2<>(SortableRelationKey.create(r, r.getSource()), r))
			.repartitionAndSortWithinPartitions(new RelationPartitioner(relPartitions))
			.groupBy(Tuple2::_1)
			.map(Tuple2::_2)
			.map(t -> Iterables.limit(t, maxRelations))
			.flatMap(Iterable::iterator)
			.map(Tuple2::_2)
			.rdd();

		// group by TARGET and apply limit
		RDD<Relation> byTarget = readPathRelationRDD(spark, inputRelationsPath)
			.filter(rel -> rel.getDataInfo().getDeletedbyinference() == false)
			.filter(rel -> relationFilter.contains(rel.getRelClass()) == false)
			.mapToPair(r -> new Tuple2<>(SortableRelationKey.create(r, r.getTarget()), r))
			.repartitionAndSortWithinPartitions(new RelationPartitioner(relPartitions))
			.groupBy(Tuple2::_1)
			.map(Tuple2::_2)
			.map(t -> Iterables.limit(t, maxRelations))
			.flatMap(Iterable::iterator)
			.map(Tuple2::_2)
			.rdd();

		spark
			.createDataset(bySource.union(byTarget), Encoders.bean(Relation.class))
			.repartition(relPartitions)
			.write()
			.mode(SaveMode.Overwrite)
			.parquet(outputPath);
	}

	private static void prepareRelationsDataset(
		SparkSession spark, String inputRelationsPath, String outputPath, Set<String> relationFilter, int maxRelations,
		int relPartitions) {

		Dataset<Relation> bySource = pruneRelations(
			spark, inputRelationsPath, relationFilter, maxRelations, relPartitions,
			(Function<Relation, String>) r -> r.getSource());
		Dataset<Relation> byTarget = pruneRelations(
			spark, inputRelationsPath, relationFilter, maxRelations, relPartitions,
			(Function<Relation, String>) r -> r.getTarget());

		bySource
			.union(byTarget)
			.repartition(relPartitions)
			.write()
			.mode(SaveMode.Overwrite)
			.parquet(outputPath);
	}

	private static Dataset<Relation> pruneRelations(SparkSession spark, String inputRelationsPath,
		Set<String> relationFilter, int maxRelations, int relPartitions,
		Function<Relation, String> idFn) {
		return readRelations(spark, inputRelationsPath, relationFilter, relPartitions)
			.groupByKey(
				(MapFunction<Relation, String>) r -> idFn.call(r),
				Encoders.STRING())
			.agg(new RelationAggregator(maxRelations).toColumn())
			.flatMap(
				(FlatMapFunction<Tuple2<String, RelationList>, Relation>) t -> t
					._2()
					.getRelations()
					.iterator(),
				Encoders.bean(Relation.class));
	}

	private static Dataset<Relation> readRelations(SparkSession spark, String inputRelationsPath,
		Set<String> relationFilter, int relPartitions) {
		return spark
			.read()
			.textFile(inputRelationsPath)
			.repartition(relPartitions)
			.map(
				(MapFunction<String, Relation>) s -> OBJECT_MAPPER.readValue(s, Relation.class),
				Encoders.kryo(Relation.class))
			.filter((FilterFunction<Relation>) rel -> rel.getDataInfo().getDeletedbyinference() == false)
			.filter((FilterFunction<Relation>) rel -> relationFilter.contains(rel.getRelClass()) == false);
	}

	public static class RelationAggregator
		extends Aggregator<Relation, RelationList, RelationList> {

		private int maxRelations;

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
