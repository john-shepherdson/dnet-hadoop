
package eu.dnetlib.dhp.actionmanager.project;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.csvutils.CSVProgramme;
import eu.dnetlib.dhp.actionmanager.project.csvutils.CSVProject;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import scala.Tuple2;

public class PrepareProjects {

	private static final Logger log = LoggerFactory.getLogger(PrepareProgramme.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final HashMap<String, CSVProgramme> programmeMap = new HashMap<>();

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

		final String  dbProjectPath = parser.get("dbProjectPath");
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

	private static void exec(SparkSession spark, String progjectPath, String dbProjectPath, String outputPath) {
		Dataset<CSVProject> project = readPath(spark, progjectPath, CSVProject.class);
		Dataset<ProjectSubset> dbProjects = readPath(spark, dbProjectPath, ProjectSubset.class);

		dbProjects.joinWith(project, dbProjects.col("code").equalTo(project.col("id")), "left")
				.flatMap((FlatMapFunction<Tuple2<ProjectSubset, CSVProject>, CSVProject>) value -> {
					Optional<CSVProject> csvProject = Optional.ofNullable(value._2());
					if(! csvProject.isPresent()){
						return null;
					}
					List<CSVProject> csvProjectList = new ArrayList<>();
					String[] programme = csvProject.get().getProgramme().split(";");
					Arrays
							.stream(programme)
							.forEach(p -> {
								CSVProject proj = new CSVProject();
								proj.setProgramme(p);
								proj.setId(csvProject.get().getId());
								csvProjectList.add(proj);
							});

					return csvProjectList.iterator();
				}, Encoders.bean(CSVProject.class))
		.write()
		.mode(SaveMode.Overwrite)
		.option("compression", "gzip")
		.json(outputPath);
//
//				.map(value -> {
//					Optional<CSVProject> csvProject = Optional.ofNullable(value._2());
//				}, Encoders.bean(CSVProject.class))
//		.filter(Objects::nonNull)
//			.toJavaRDD()
//			.flatMap(p -> {
//				List<CSVProject> csvProjectList = new ArrayList<>();
//				String[] programme = p.getProgramme().split(";");
//				Arrays
//					.stream(programme)
//					.forEach(value -> {
//						CSVProject csvProject = new CSVProject();
//						csvProject.setProgramme(value);
//						csvProject.setId(p.getId());
//						csvProjectList.add(csvProject);
//					});
//
//				return csvProjectList.iterator();
//			})
//			.map(p -> OBJECT_MAPPER.writeValueAsString(p))
//			.saveAsTextFile(outputPath);

	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}
}
