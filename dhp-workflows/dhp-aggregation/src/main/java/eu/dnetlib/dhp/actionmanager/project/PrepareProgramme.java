
package eu.dnetlib.dhp.actionmanager.project;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.HashMap;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.csvutils.CSVProgramme;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import scala.Tuple2;

public class PrepareProgramme {

	private static final Logger log = LoggerFactory.getLogger(PrepareProgramme.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final HashMap<String, CSVProgramme> programmeMap = new HashMap<>();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareProgramme.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/project/prepare_programme_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String programmePath = parser.get("programmePath");
		log.info("programmePath {}: ", programmePath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				exec(spark, programmePath, outputPath);
			});
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	private static void exec(SparkSession spark, String programmePath, String outputPath) {
		Dataset<CSVProgramme> programme = readPath(spark, programmePath, CSVProgramme.class);

		programme
			.toJavaRDD()
			.filter(p -> !p.getCode().contains("FP7"))
			.mapToPair(csvProgramme -> new Tuple2<>(csvProgramme.getCode(), csvProgramme))
			.reduceByKey((a, b) -> {
				if (StringUtils.isEmpty(a.getShortTitle())) {
					if (StringUtils.isEmpty(b.getShortTitle())) {
						if (StringUtils.isEmpty(a.getTitle())) {
							if (StringUtils.isNotEmpty(b.getTitle())) {
								a.setShortTitle(b.getTitle());
								a.setLanguage(b.getLanguage());
							}
						} else {// notIsEmpty a.getTitle
							if (StringUtils.isEmpty(b.getTitle())) {
								a.setShortTitle(a.getTitle());
							} else {
								if (b.getLanguage().equalsIgnoreCase("en")) {
									a.setShortTitle(b.getTitle());
									a.setLanguage(b.getLanguage());
								} else {
									a.setShortTitle(a.getTitle());
								}
							}
						}
					} else {// not isEmpty b.getShortTitle
						a.setShortTitle(b.getShortTitle());
						// a.setLanguage(b.getLanguage());
					}
				}
				return a;

			})
			.map(p -> {
				CSVProgramme csvProgramme = p._2();
				if (StringUtils.isEmpty(csvProgramme.getShortTitle())) {
					csvProgramme.setShortTitle(csvProgramme.getTitle());
				}
				return OBJECT_MAPPER.writeValueAsString(csvProgramme);
			})
			.saveAsTextFile(outputPath);

	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}

}
