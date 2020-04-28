
package eu.dnetlib.dhp.sx.graph;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import eu.dnetlib.dhp.schema.scholexplorer.DLIRelation;
import eu.dnetlib.dhp.utils.DHPUtils;
import scala.Tuple2;

/**
 * In some case the identifier generated for the Entity in @{@link SparkExtractEntitiesJob} is different from the
 * identifier * associated by the aggregator, this means that some relation points to missing identifier To avoid this
 * problem we store in the model the Id and the OriginalObJIdentifier This jobs extract this pair and creates a Similar
 * relation that will be used in SparkMergeEntities
 */
public class SparkSXGeneratePidSimlarity {

	static final String IDJSONPATH = "$.id";
	static final String OBJIDPATH = "$.originalObjIdentifier";

	public static void generateDataFrame(
		final SparkSession spark,
		final JavaSparkContext sc,
		final String inputPath,
		final String targetPath) {

		final JavaPairRDD<String, String> datasetSimRel = sc
			.textFile(inputPath + "/dataset/*")
			.mapToPair(
				(PairFunction<String, String, String>) k -> new Tuple2<>(
					DHPUtils.getJPathString(IDJSONPATH, k),
					DHPUtils.getJPathString(OBJIDPATH, k)))
			.filter(
				t -> !StringUtils
					.substringAfter(t._1(), "|")
					.equalsIgnoreCase(StringUtils.substringAfter(t._2(), "::")))
			.distinct();

		final JavaPairRDD<String, String> publicationSimRel = sc
			.textFile(inputPath + "/publication/*")
			.mapToPair(
				(PairFunction<String, String, String>) k -> new Tuple2<>(
					DHPUtils.getJPathString(IDJSONPATH, k),
					DHPUtils.getJPathString(OBJIDPATH, k)))
			.filter(
				t -> !StringUtils
					.substringAfter(t._1(), "|")
					.equalsIgnoreCase(StringUtils.substringAfter(t._2(), "::")))
			.distinct();

		JavaRDD<DLIRelation> simRel = datasetSimRel
			.union(publicationSimRel)
			.map(
				s -> {
					final DLIRelation r = new DLIRelation();
					r.setSource(s._1());
					r.setTarget(s._2());
					r.setRelType("similar");
					return r;
				});
		spark
			.createDataset(simRel.rdd(), Encoders.bean(DLIRelation.class))
			.distinct()
			.write()
			.mode(SaveMode.Overwrite)
			.save(targetPath + "/pid_simRel");
	}
}
