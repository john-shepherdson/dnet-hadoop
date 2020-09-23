
package eu.dnetlib.dhp.actionmanager.project;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.project.csvutils.CSVProgramme;
import eu.dnetlib.dhp.actionmanager.project.csvutils.CSVProject;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.H2020Classification;
import eu.dnetlib.dhp.schema.oaf.H2020Programme;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.utils.DHPUtils;
import scala.Tuple2;

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

	private static void getAtomicActions(SparkSession spark, String projectPatH,
		String programmePath,
		String outputPath) {

		Dataset<CSVProject> project = readPath(spark, projectPatH, CSVProject.class);
		Dataset<CSVProgramme> programme = readPath(spark, programmePath, CSVProgramme.class);

		project
			.joinWith(programme, project.col("programme").equalTo(programme.col("code")), "left")
			.map((MapFunction<Tuple2<CSVProject, CSVProgramme>, Project>) c -> {
				CSVProject csvProject = c._1();
				Optional<CSVProgramme> ocsvProgramme = Optional.ofNullable(c._2());
				if (ocsvProgramme.isPresent()) {
					Project p = new Project();
					p
						.setId(
							createOpenaireId(
								ModelSupport.entityIdPrefix.get("project"),
								"corda__h2020", csvProject.getId()));
					p.setH2020topiccode(csvProject.getTopics());
					H2020Programme pm = new H2020Programme();
					H2020Classification h2020classification = new H2020Classification();
					pm.setCode(csvProject.getProgramme());
					CSVProgramme csvProgramme = ocsvProgramme.get();
					if (StringUtils.isNotEmpty(csvProgramme.getShortTitle())) {
						pm.setDescription(csvProgramme.getShortTitle());
					} else {
						pm.setDescription(csvProgramme.getTitle());
					}
					h2020classification.setClassification(ocsvProgramme.get().getClassification());
					setLevels(h2020classification, ocsvProgramme.get().getClassification());
					h2020classification.setH2020Programme(pm);
					p.setH2020classification(Arrays.asList(h2020classification));
					return p;
				}

				return null;
			}, Encoders.bean(Project.class))
			.filter(Objects::nonNull)
			.groupByKey(
				(MapFunction<Project, String>) p -> p.getId(),
				Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, Project, Project>) (s, it) -> {
				Project first = it.next();
				it.forEachRemaining(p -> {
					first.mergeFrom(p);
				});
				return first;
			}, Encoders.bean(Project.class))
			.toJavaRDD()
			.map(p -> new AtomicAction(Project.class, p))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class);

	}

	private static void setLevels(H2020Classification h2020Classification, String classification) {
		String[] tmp = classification.split(" | ");
		h2020Classification.setLevel1(tmp[0]);
		if (tmp.length > 1) {
			h2020Classification.setLevel2(tmp[1]);
		}
		if (tmp.length > 2) {
			h2020Classification.setLevel3(tmp[2]);
		}
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
