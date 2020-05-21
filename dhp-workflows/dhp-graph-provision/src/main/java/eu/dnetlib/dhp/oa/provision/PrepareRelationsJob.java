
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.provision.model.SortableRelation;
import eu.dnetlib.dhp.oa.provision.utils.RelationPartitioner;
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

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareRelationsRDD(
					spark, inputRelationsPath, outputPath, relationFilter, relPartitions, maxRelations);
			});
	}

	/**
	 * Dataset based implementation that prepares the graph relations by limiting the number of outgoing links and
	 * filtering the relation types according to the given criteria.
	 *
	 * @param spark the spark session
	 * @param inputRelationsPath source path for the graph relations
	 * @param outputPath output path for the processed relations
	 * @param relationFilter set of relation filters applied to the `relClass` field
	 * @param maxRelations maximum number of allowed outgoing edges
	 */
	private static void prepareRelations(
		SparkSession spark, String inputRelationsPath, String outputPath, Set<String> relationFilter,
		int maxRelations) {
		readPathRelation(spark, inputRelationsPath)
			.filter("dataInfo.deletedbyinference == false")
			.filter((FilterFunction<SortableRelation>) rel -> !relationFilter.contains(rel.getRelClass()))
			.groupByKey(
				(MapFunction<SortableRelation, String>) value -> value.getSource(), Encoders.STRING())
			.flatMapGroups(
				(FlatMapGroupsFunction<String, SortableRelation, SortableRelation>) (key, values) -> Iterators
					.limit(values, maxRelations),
				Encoders.bean(SortableRelation.class))
			.write()
			.mode(SaveMode.Overwrite)
			.parquet(outputPath);
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
	 */
	// TODO work in progress
	private static void prepareRelationsRDD(
		SparkSession spark, String inputRelationsPath, String outputPath, Set<String> relationFilter, int relPartitions,
		int maxRelations) {
		JavaRDD<SortableRelation> rels = readPathRelationRDD(spark, inputRelationsPath).repartition(relPartitions);
		RelationPartitioner partitioner = new RelationPartitioner(rels.getNumPartitions());

		// only consider those that are not virtually deleted
		RDD<SortableRelation> d = rels
			.filter(rel -> !rel.getDataInfo().getDeletedbyinference())
			.filter(rel -> !relationFilter.contains(rel.getRelClass()))
			.mapToPair(
				(PairFunction<SortableRelation, SortableRelation, SortableRelation>) rel -> new Tuple2<>(rel, rel))
			.groupByKey(partitioner)
			.map(group -> Iterables.limit(group._2(), maxRelations))
			.flatMap(group -> group.iterator())
			.rdd();

		spark
			.createDataset(d, Encoders.bean(SortableRelation.class))
			.write()
			.mode(SaveMode.Overwrite)
			.parquet(outputPath);
	}

	/**
	 * Reads a Dataset of eu.dnetlib.dhp.oa.provision.model.SortableRelation objects from a newline delimited json text
	 * file,
	 *
	 * @param spark
	 * @param inputPath
	 * @return the Dataset<SortableRelation> containing all the relationships
	 */
	private static Dataset<SortableRelation> readPathRelation(
		SparkSession spark, final String inputPath) {
		return spark
			.read()
			.textFile(inputPath)
			.map(
				(MapFunction<String, SortableRelation>) value -> OBJECT_MAPPER.readValue(value, SortableRelation.class),
				Encoders.bean(SortableRelation.class));
	}

	private static JavaRDD<SortableRelation> readPathRelationRDD(
		SparkSession spark, final String inputPath) {
		JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
		return sc.textFile(inputPath).map(s -> OBJECT_MAPPER.readValue(s, SortableRelation.class));
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}
}
