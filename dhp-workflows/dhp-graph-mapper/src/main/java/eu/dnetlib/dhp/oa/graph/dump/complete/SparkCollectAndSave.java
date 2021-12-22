
package eu.dnetlib.dhp.oa.graph.dump.complete;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.oaf.graph.GraphResult;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Relation;

/**
 * Reads all the entities of the same type (Relation / Results) and saves them in the same folder
 */
public class SparkCollectAndSave implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkCollectAndSave.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkCollectAndSave.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/input_collect_and_save.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final Boolean aggregateResult = Optional
			.ofNullable(parser.get("resultAggregation"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath + "/result");
				run(spark, inputPath, outputPath, aggregateResult);

			});

	}

	private static void run(SparkSession spark, String inputPath, String outputPath, boolean aggregate) {
		if (aggregate) {
			Utils
				.readPath(spark, inputPath + "/result/publication", GraphResult.class)
				.union(Utils.readPath(spark, inputPath + "/result/dataset", GraphResult.class))
				.union(Utils.readPath(spark, inputPath + "/result/otherresearchproduct", GraphResult.class))
				.union(Utils.readPath(spark, inputPath + "/result/software", GraphResult.class))
				.write()
				.option("compression", "gzip")
				.mode(SaveMode.Overwrite)
				.json(outputPath + "/result");
		} else {
			write(
				Utils
					.readPath(spark, inputPath + "/result/publication", GraphResult.class),
				outputPath + "/publication");
			write(
				Utils
					.readPath(spark, inputPath + "/result/dataset", GraphResult.class),
				outputPath + "/dataset");
			write(
				Utils
					.readPath(spark, inputPath + "/result/otherresearchproduct", GraphResult.class),
				outputPath + "/otheresearchproduct");
			write(
				Utils
					.readPath(spark, inputPath + "/result/software", GraphResult.class),
				outputPath + "/software");

		}

		Utils
			.readPath(spark, inputPath + "/relation/publication", Relation.class)
			.union(Utils.readPath(spark, inputPath + "/relation/dataset", Relation.class))
			.union(Utils.readPath(spark, inputPath + "/relation/orp", Relation.class))
			.union(Utils.readPath(spark, inputPath + "/relation/software", Relation.class))
			.union(Utils.readPath(spark, inputPath + "/relation/contextOrg", Relation.class))
			.union(Utils.readPath(spark, inputPath + "/relation/context", Relation.class))
			.union(Utils.readPath(spark, inputPath + "/relation/relation", Relation.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/relation");

	}

	private static void write(Dataset<GraphResult> dataSet, String outputPath) {
		dataSet
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(outputPath);
	}
}
