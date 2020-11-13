
package eu.dnetlib.dhp.oa.graph.groupbyid;

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
import eu.dnetlib.dhp.oa.graph.raw.MigrateMongoMdstoresApplication;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import scala.Tuple2;

public class DispatchEntitiesSparkJob {

	private static final Logger log = LoggerFactory.getLogger(DispatchEntitiesSparkJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					MigrateMongoMdstoresApplication.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/graph/dispatch_entities_bytype_parameters.json")));
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String entitiesPath = parser.get("entitiesPath");
		log.info("entitiesPath: {}", entitiesPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		String graphTableClassName = parser.get("graphTableClassName");
		log.info("graphTableClassName: {}", graphTableClassName);

		Class<? extends OafEntity> entityClazz = (Class<? extends OafEntity>) Class.forName(graphTableClassName);

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ModelSupport.getOafModelClasses());

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				HdfsSupport.remove(outputPath, spark.sparkContext().hadoopConfiguration());
				dispatchOaf(spark, entityClazz, entitiesPath, outputPath);
			});
	}

	private static <T extends Oaf> void dispatchOaf(
		final SparkSession spark,
		final Class<T> clazz,
		final String sourcePath,
		final String targetPath) {

		log.info("Processing entities ({}) in file: {}", clazz.getName(), sourcePath);

		spark
			.read()
			.textFile(sourcePath)
			.filter((FilterFunction<String>) s -> isEntityType(s, clazz))
			.map((MapFunction<String, String>) s -> StringUtils.substringAfter(s, "|"), Encoders.STRING())
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.text(targetPath);
	}

	private static <T extends Oaf> boolean isEntityType(final String s, final Class<T> clazz) {
		return StringUtils.substringBefore(s, "|").equals(clazz.getName());
	}

}
