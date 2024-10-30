
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.FileNotFoundException;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.raw.common.RelationIdMapping;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

public class PatchRelationsApplication {

	private static final Logger log = LoggerFactory.getLogger(PatchRelationsApplication.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Optional
						.ofNullable(
							PatchRelationsApplication.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/oa/graph/patch_relations_parameters.json"))
						.orElseThrow(FileNotFoundException::new)));
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String graphBasePath = parser.get("graphBasePath");
		log.info("graphBasePath: {}", graphBasePath);

		final String workingDir = parser.get("workingDir");
		log.info("workingDir: {}", workingDir);

		final String idMappingPath = parser.get("idMappingPath");
		log.info("idMappingPath: {}", idMappingPath);

		final SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> patchRelations(spark, graphBasePath, workingDir, idMappingPath));
	}

	/**
	 * Substitutes the identifiers (source/target) from the set of relations part of the graphBasePath included in the
	 * mapping provided by the dataset stored on idMappingPath, using workingDir as intermediate storage location.
	 *
	 * @param spark the SparkSession
	 * @param graphBasePath base graph path providing the set of relations to patch
	 * @param workingDir intermediate storage location
	 * @param idMappingPath dataset providing the old -> new identifier mapping
	 */
	private static void patchRelations(final SparkSession spark, final String graphBasePath, final String workingDir,
		final String idMappingPath) {

		final String relationPath = graphBasePath + "/relation";

		final Dataset<Relation> rels = readPath(spark, relationPath, Relation.class);
		final Dataset<RelationIdMapping> idMapping = readPath(spark, idMappingPath, RelationIdMapping.class);

		final Dataset<Relation> bySource = rels
			.joinWith(idMapping, rels.col("source").equalTo(idMapping.col("oldId")), "left")
			.map((MapFunction<Tuple2<Relation, RelationIdMapping>, Relation>) t -> {
				final Relation r = t._1();
				Optional
					.ofNullable(t._2())
					.map(RelationIdMapping::getNewId)
					.ifPresent(r::setSource);
				return r;
			}, Encoders.bean(Relation.class));

		bySource
			.joinWith(idMapping, bySource.col("target").equalTo(idMapping.col("oldId")), "left")
			.map((MapFunction<Tuple2<Relation, RelationIdMapping>, Relation>) t -> {
				final Relation r = t._1();
				Optional
					.ofNullable(t._2())
					.map(RelationIdMapping::getNewId)
					.ifPresent(r::setTarget);
				return r;
			}, Encoders.bean(Relation.class))
			.map(
				(MapFunction<Relation, String>) OBJECT_MAPPER::writeValueAsString,
				Encoders.STRING())
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.text(workingDir);

		spark
			.read()
			.textFile(workingDir)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.text(relationPath);
	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}

}
