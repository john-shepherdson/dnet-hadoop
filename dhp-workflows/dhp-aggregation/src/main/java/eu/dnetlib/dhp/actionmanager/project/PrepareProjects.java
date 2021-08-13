
package eu.dnetlib.dhp.actionmanager.project;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.utils.model.CSVProject;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import scala.Tuple2;

/**
 * Selects only the relevant information collected with the projects: project grant agreement, project programme code and
 * project topic code for the projects that are also collected from OpenAIRE.
 */
public class PrepareProjects {

	private static final Logger log = LoggerFactory.getLogger(PrepareProjects.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareProjects.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/project/prepare_project_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String projectPath = parser.get("projectPath");
		log.info("projectPath {}: ", projectPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		final String dbProjectPath = parser.get("dbProjectPath");
		log.info("dbProjectPath {}: ", dbProjectPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				exec(spark, projectPath, dbProjectPath, outputPath);
			});
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	private static void exec(SparkSession spark, String projectPath, String dbProjectPath, String outputPath) {
		Dataset<CSVProject> project = readPath(spark, projectPath, CSVProject.class);
		Dataset<ProjectSubset> dbProjects = readPath(spark, dbProjectPath, ProjectSubset.class);

		dbProjects
			.joinWith(project, dbProjects.col("code").equalTo(project.col("id")), "left")
			.flatMap(getTuple2CSVProjectFlatMapFunction(), Encoders.bean(CSVProject.class))
			.filter(Objects::nonNull)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);

	}

	private static FlatMapFunction<Tuple2<ProjectSubset, CSVProject>, CSVProject> getTuple2CSVProjectFlatMapFunction() {
		return value -> {
			Optional<CSVProject> csvProject = Optional.ofNullable(value._2());
			List<CSVProject> csvProjectList = new ArrayList<>();
			if (csvProject.isPresent()) {

				String[] programme = csvProject.get().getProgramme().split(";");
				String topic = csvProject.get().getTopics();

				Arrays
					.stream(programme)
					.forEach(p -> {
						CSVProject proj = new CSVProject();
						proj.setTopics(topic);

						proj.setProgramme(p);
						proj.setId(csvProject.get().getId());
						csvProjectList.add(proj);
					});
			}
			return csvProjectList.iterator();
		};
	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}
}
