
package eu.dnetlib.dhp.oa.dedup;

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

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;

public class DispatchEntitiesSparkJob {

	private static final Logger log = LoggerFactory.getLogger(DispatchEntitiesSparkJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				DispatchEntitiesSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/dedup/dispatch_entities_parameters.json"));
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

		String graphTableClassName = parser.get("graphTableClassName");
		log.info("graphTableClassName: {}", graphTableClassName);

		Class<? extends OafEntity> entityClazz = (Class<? extends OafEntity>) Class.forName(graphTableClassName);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				HdfsSupport.remove(outputPath, spark.sparkContext().hadoopConfiguration());
				dispatchEntities(spark, inputPath, entityClazz, outputPath);
			});
	}

	private static <T extends Oaf> void dispatchEntities(
		SparkSession spark,
		String inputPath,
		Class<T> clazz,
		String outputPath) {

		spark
			.read()
			.textFile(inputPath)
			.filter((FilterFunction<String>) s -> isEntityType(s, clazz))
			.map((MapFunction<String, String>) s -> StringUtils.substringAfter(s, "|"), Encoders.STRING())
			.map(
				(MapFunction<String, T>) value -> OBJECT_MAPPER.readValue(value, clazz),
				Encoders.bean(clazz))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

	private static <T extends Oaf> boolean isEntityType(final String s, final Class<T> clazz) {
		return StringUtils.substringBefore(s, "|").equals(clazz.getName());
	}

}
