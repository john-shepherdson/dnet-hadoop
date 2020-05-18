
package eu.dnetlib.dhp.actionmanager.project;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;

public class SparkAtomicActionJob {
	private static final Logger log = LoggerFactory.getLogger(SparkAtomicActionJob.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkAtomicActionJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/project/action_set_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String projectPath = parser.get("projectPath");
		log.info("projectPath: {}", projectPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		final String programmePath = parser.get("programmePath");
		log.info("programmePath {}: ", programmePath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				getAtomicActions(
					spark,
					projectPath,
					programmePath,
					outputPath);
			});
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	private static void getAtomicActions(SparkSession spark, String projectPatj, String programmePath,
		String outputPath) {

	}
}
