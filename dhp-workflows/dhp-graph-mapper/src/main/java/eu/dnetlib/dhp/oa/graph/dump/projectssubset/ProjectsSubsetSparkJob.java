
package eu.dnetlib.dhp.oa.graph.dump.projectssubset;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.oaf.community.Funder;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Project;
import scala.Tuple2;

public class ProjectsSubsetSparkJob implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(ProjectsSubsetSparkJob.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				ProjectsSubsetSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/project_subset_parameters.json"));

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

		final String projectListPath = parser.get("projectListPath");
		log.info("projectListPath: {}", projectListPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				getNewProjectList(spark, inputPath, outputPath, projectListPath);
			});
	}

	private static void getNewProjectList(SparkSession spark, String inputPath, String outputPath,
		String projectListPath) {

		Dataset<String> projectList = spark.read().textFile(projectListPath);

		Dataset<Project> projects;
		projects = Utils.readPath(spark, inputPath, Project.class);

		projects
			.joinWith(projectList, projects.col("id").equalTo(projectList.col("value")), "left")
			.map((MapFunction<Tuple2<Project, String>, Project>) t2 -> {
				if (Optional.ofNullable(t2._2()).isPresent())
					return null;
				return t2._1();
			}, Encoders.bean(Project.class))
			.filter(Objects::nonNull)
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);

		Utils
			.readPath(spark, outputPath, Project.class)
			.map((MapFunction<Project, String>) p -> p.getId(), Encoders.STRING())
			.write()
			.mode(SaveMode.Append)
			.option("compression", "gzip")
			.text(projectListPath);

	}

}
