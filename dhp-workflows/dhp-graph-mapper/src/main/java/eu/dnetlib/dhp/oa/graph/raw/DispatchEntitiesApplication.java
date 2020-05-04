
package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;

public class DispatchEntitiesApplication {

	private static final Logger log = LoggerFactory.getLogger(DispatchEntitiesApplication.class);

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					MigrateMongoMdstoresApplication.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/graph/dispatch_entities_parameters.json")));
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String sourcePath = parser.get("sourcePath");
		final String targetPath = parser.get("graphRawPath");

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, targetPath);
				ModelSupport.oafTypes
					.values()
					.forEach(clazz -> processEntity(spark, clazz, sourcePath, targetPath));
			});
	}

	private static <T extends Oaf> void processEntity(
		final SparkSession spark,
		final Class<T> clazz,
		final String sourcePath,
		final String targetPath) {
		final String type = clazz.getSimpleName().toLowerCase();

		log.info("Processing entities ({}) in file: {}", type, sourcePath);

		spark
			.read()
			.textFile(sourcePath)
			.filter((FilterFunction<String>) value -> isEntityType(value, type))
			.map(
				(MapFunction<String, String>) l -> StringUtils.substringAfter(l, "|"),
				Encoders.STRING())
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.text(targetPath + "/" + type);
	}

	private static boolean isEntityType(final String line, final String type) {
		return StringUtils.substringBefore(line, "|").equalsIgnoreCase(type);
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}
}
