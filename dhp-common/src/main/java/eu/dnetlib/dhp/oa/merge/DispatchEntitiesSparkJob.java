
package eu.dnetlib.dhp.oa.merge;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.common.ModelSupport;

public class DispatchEntitiesSparkJob {

	private static final Logger log = LoggerFactory.getLogger(DispatchEntitiesSparkJob.class);

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				Objects
					.requireNonNull(
						DispatchEntitiesSparkJob.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/oa/merge/dispatch_entities_parameters.json")));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
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

		boolean filterInvisible = Boolean.valueOf(parser.get("filterInvisible"));
		log.info("filterInvisible: {}", filterInvisible);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				HdfsSupport.remove(outputPath, spark.sparkContext().hadoopConfiguration());
				dispatchEntities(spark, inputPath, outputPath, filterInvisible);
			});
	}

	private static void dispatchEntities(
		SparkSession spark,
		String inputPath,
		String outputPath,
		boolean filterInvisible) {

		Dataset<String> df = spark.read().textFile(inputPath);

		ModelSupport.oafTypes.entrySet().parallelStream().forEach(entry -> {
			String entityType = entry.getKey();
			Class<?> clazz = entry.getValue();

			if (!entityType.equalsIgnoreCase("relation")) {
				Dataset<Row> entityDF = spark
					.read()
					.schema(Encoders.bean(clazz).schema())
					.json(
						df
							.filter((FilterFunction<String>) s -> s.startsWith(clazz.getName()))
							.map(
								(MapFunction<String, String>) s -> StringUtils.substringAfter(s, "|"),
								Encoders.STRING()));

				if (filterInvisible) {
					entityDF = entityDF.filter("dataInfo.invisible != true");
				}

				entityDF
					.write()
					.mode(SaveMode.Overwrite)
					.option("compression", "gzip")
					.json(outputPath + "/" + entityType);
			}
		});
	}
}
