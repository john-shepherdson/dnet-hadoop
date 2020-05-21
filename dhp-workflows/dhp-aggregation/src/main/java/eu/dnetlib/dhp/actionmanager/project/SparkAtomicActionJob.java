
package eu.dnetlib.dhp.actionmanager.project;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.function.Consumer;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.spark.SparkConf;
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
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.Programme;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.utils.DHPUtils;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.SequenceFile;
import org.apache.spark.rdd.SequenceFileRDDFunctions;
import org.apache.hadoop.io.Text;
import scala.Function1;
import scala.Tuple2;
import scala.runtime.BoxedUnit;

public class SparkAtomicActionJob {
	private static final Logger log = LoggerFactory.getLogger(SparkAtomicActionJob.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final HashMap<String, String> programmeMap = new HashMap<>();

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

		final String nameNode = parser.get("hdfsNameNode");

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
					outputPath,
						nameNode);
			});
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	private static void getAtomicActions(SparkSession spark, String projectPatH,
		String programmePath,
		String outputPath,
										 String nameNode) throws Exception{

		Dataset<CSVProject> project = readPath(spark, projectPatH, CSVProject.class);
		Dataset<CSVProgramme> programme = readPath(spark, programmePath, CSVProgramme.class);

		project
				.joinWith(programme, project.col("programme").equalTo(programme.col("code")), "left")
				.map(c -> {
					CSVProject csvProject = c._1();
					Optional<CSVProgramme> csvProgramme = Optional.ofNullable(c._2());
					if (csvProgramme.isPresent()) {
						Project p = new Project();
						p
								.setId(
										createOpenaireId(
												ModelSupport.entityIdPrefix.get("project"),
												"corda__h2020", csvProject.getId()));
						Programme pm = new Programme();
						pm.setCode(csvProject.getProgramme());
						pm.setDescription(csvProgramme.get().getShortTitle());
						p.setProgramme(Arrays.asList(pm));
						return new AtomicAction<>(Project.class, p);
					}

					return null;
				}, Encoders.bean(AtomicAction.class))
				.filter(aa -> !(aa == null))
				.toJavaRDD()
				.mapToPair(aa->new Tuple2<>(aa.getClazz().getCanonicalName(), OBJECT_MAPPER.writeValueAsString(aa)))
				.saveAsHadoopFile(outputPath, Text.class, Text.class, null);

	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}

	public static String createOpenaireId(
		final String prefix, final String nsPrefix, final String id) {

		return String.format("%s|%s::%s", prefix, nsPrefix, DHPUtils.md5(id));

	}
}
