
package eu.dnetlib.dhp.oa.provision;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;
import static org.apache.spark.sql.functions.col;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.utils.DHPUtils;
import eu.dnetlib.dhp.utils.InputType;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.oa.provision.model.ProvisionModelSupport;
import eu.dnetlib.dhp.schema.oaf.Relation;

/**
 * PrepareRelationsJob prunes the relationships: only consider relationships that are not virtually deleted
 * ($.dataInfo.deletedbyinference == false), each entity can be linked at most to 100 other objects
 */
public class PrepareRelationsJob {

	private static final Logger log = LoggerFactory.getLogger(PrepareRelationsJob.class);

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

        InputType inputType = Optional.ofNullable(parser.get("inputType"))
                .map(InputType::valueOf)
                .orElse(InputType.HDFS_JSON);
        log.info("inputType: {}", inputType);

		String inputGraph = parser.get("inputGraph");
		log.info("inputGraph: {}", inputGraph);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

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

        String hiveMetastoreUris = parser.get("hiveMetastoreUris");
        log.info("hiveMetastoreUris: {}", hiveMetastoreUris);

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", hiveMetastoreUris);
        conf.set("spark.hadoop.hive.metastore.uris", hiveMetastoreUris);
        conf.set("spark.sql.catalogImplementation", "hive");

		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ProvisionModelSupport.getModelClasses());

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareRelationsRDD(
					spark, inputType, inputGraph, outputPath, relationFilter, sourceMaxRelations, targetMaxRelations);
			});
	}

	/**
	 * RDD based implementation that prepares the graph relations by limiting the number of outgoing links and filtering
	 * the relation types according to the given criteria. Moreover, outgoing links kept within the given limit are
	 * prioritized according to the weights indicated in eu.dnetlib.dhp.oa.provision.model.SortableRelation.
	 *
	 * @param spark the spark session
     * @param inputType type of input relations
	 * @param inputGraph source path for the graph relations
	 * @param outputPath output path for the processed relations
	 * @param relationFilter set of relation filters applied to the `relClass` field
	 * @param sourceMaxRelations maximum number of allowed outgoing edges grouping by relation.source
	 * @param targetMaxRelations maximum number of allowed outgoing edges grouping by relation.target
	 */
	private static void prepareRelationsRDD(SparkSession spark, InputType inputType, String inputGraph, String outputPath,
                                            Set<String> relationFilter, int sourceMaxRelations, int targetMaxRelations) {

		WindowSpec source_w = Window
			.partitionBy("source", "subRelType")
			.orderBy(col("target").desc_nulls_last());

		WindowSpec target_w = Window
			.partitionBy("target", "subRelType")
			.orderBy(col("source").desc_nulls_last());

        DHPUtils.readGraph(spark, inputType, inputGraph, Relation.class)
			.where("source NOT LIKE 'unresolved%' AND  target  NOT LIKE 'unresolved%'")
			.where("datainfo.deletedbyinference != true")
			.where(
				relationFilter.isEmpty() ? ""
					: "lower(relClass) NOT IN ("
						+ relationFilter.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")) + ")")
			.withColumn("source_w_pos", functions.row_number().over(source_w))
			.where("source_w_pos < " + sourceMaxRelations)
			.drop("source_w_pos")
			.withColumn("target_w_pos", functions.row_number().over(target_w))
			.where("target_w_pos < " + targetMaxRelations)
			.drop("target_w_pos")
			.write()
			.mode(SaveMode.Overwrite)
			.parquet(outputPath);
	}

    private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}
}
