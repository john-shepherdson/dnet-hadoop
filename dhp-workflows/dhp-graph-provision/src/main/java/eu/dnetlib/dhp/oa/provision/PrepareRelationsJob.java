
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.provision.model.SortableRelation;
import eu.dnetlib.dhp.oa.provision.utils.RelationPartitioner;
import java.util.Optional;
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

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareRelationsFromPaths(spark, inputRelationsPath, outputPath);
			});
	}

	private static void prepareRelationsFromPaths(
		SparkSession spark, String inputRelationsPath, String outputPath) {
		readPathRelation(spark, inputRelationsPath)
			.filter("dataInfo.deletedbyinference == false")
			.groupByKey(
				(MapFunction<SortableRelation, String>) value -> value.getSource(), Encoders.STRING())
			.flatMapGroups(
				(FlatMapGroupsFunction<String, SortableRelation, SortableRelation>) (key, values) -> Iterators
					.limit(values, MAX_RELS),
				Encoders.bean(SortableRelation.class))
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

	// TODO work in progress
	private static void prepareRelationsRDDFromPaths(
		SparkSession spark, String inputRelationsPath, String outputPath, int numPartitions) {
		JavaRDD<SortableRelation> rels = readPathRelationRDD(spark, inputRelationsPath).repartition(numPartitions);

		RDD<SortableRelation> d = rels
			.filter(rel -> !rel.getDataInfo().getDeletedbyinference()) // only
			// consider
			// those
			// that are not virtually
			// deleted
			.mapToPair(
				(PairFunction<SortableRelation, SortableRelation, SortableRelation>) rel -> new Tuple2<>(rel, rel))
			.groupByKey(new RelationPartitioner(rels.getNumPartitions()))
			.map(p -> Iterables.limit(p._2(), MAX_RELS))
			.flatMap(p -> p.iterator())
			.rdd();

		spark
			.createDataset(d, Encoders.bean(SortableRelation.class))
			.write()
			.mode(SaveMode.Overwrite)
			.parquet(outputPath);
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
