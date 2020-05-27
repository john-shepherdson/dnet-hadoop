
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.provision.model.EntityRelEntity;
import eu.dnetlib.dhp.oa.provision.model.JoinedEntity;
import eu.dnetlib.dhp.oa.provision.model.Tuple2;
import eu.dnetlib.dhp.schema.common.ModelSupport;

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
public class AdjacencyListBuilderJob {

	private static final Logger log = LoggerFactory.getLogger(AdjacencyListBuilderJob.class);

	public static final int MAX_LINKS = 100;

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					AdjacencyListBuilderJob.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/provision/input_params_build_adjacency_lists.json")));
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				createAdjacencyListsRDD(spark, inputPath, outputPath);
			});
	}

	private static void createAdjacencyLists(
		SparkSession spark, String inputPath, String outputPath) {

		log.info("Reading joined entities from: {}", inputPath);
		spark
			.read()
			.load(inputPath)
			.as(Encoders.bean(EntityRelEntity.class))
			.groupByKey(
				(MapFunction<EntityRelEntity, String>) value -> value.getEntity().getId(),
				Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, EntityRelEntity, JoinedEntity>) (key, values) -> {
					JoinedEntity j = new JoinedEntity();
					List<Tuple2> links = new ArrayList<>();
					while (values.hasNext() && links.size() < MAX_LINKS) {
						EntityRelEntity curr = values.next();
						if (j.getEntity() == null) {
							j.setEntity(curr.getEntity());
						}
						links.add(new Tuple2(curr.getRelation(), curr.getTarget()));
					}
					j.setLinks(links);
					return j;
				},
				Encoders.bean(JoinedEntity.class))
			.write()
			.mode(SaveMode.Overwrite)
			.parquet(outputPath);
	}

	private static void createAdjacencyListsRDD(
		SparkSession spark, String inputPath, String outputPath) {

		log.info("Reading joined entities from: {}", inputPath);
		RDD<JoinedEntity> joinedEntities = spark
			.read()
			.load(inputPath)
			.as(Encoders.bean(EntityRelEntity.class))
			.javaRDD()
			.mapToPair(re -> {
				JoinedEntity je = new JoinedEntity();
				je.setEntity(re.getEntity());
				je.setLinks(Lists.newArrayList());
				if (re.getRelation() != null && re.getTarget() != null) {
					je.getLinks().add(new Tuple2(re.getRelation(), re.getTarget()));
				}
				return new scala.Tuple2<>(re.getEntity().getId(), je);
			})
			.reduceByKey((je1, je2) -> {
				je1.getLinks().addAll(je2.getLinks());
				return je1;
			})
			.map(t -> t._2())
			.rdd();

		spark
			.createDataset(joinedEntities, Encoders.bean(JoinedEntity.class))
			.write()
			.mode(SaveMode.Overwrite)
			.parquet(outputPath);
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}
}
