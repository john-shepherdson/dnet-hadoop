
package eu.dnetlib.dhp.actionmanager.project;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
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

import eu.dnetlib.dhp.actionmanager.project.utils.model.CSVProgramme;
import eu.dnetlib.dhp.actionmanager.project.utils.model.CSVProject;
import eu.dnetlib.dhp.actionmanager.project.utils.model.EXCELTopic;
import eu.dnetlib.dhp.actionmanager.project.utils.model.JsonTopic;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.H2020Classification;
import eu.dnetlib.dhp.schema.oaf.H2020Programme;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.utils.DHPUtils;
import scala.Tuple2;

/**
 * Class that makes the ActionSet. To prepare the AS two joins are needed
 *
 *  1. join betweem the collected project subset and the programme extenden with the classification on the grant agreement.
 *     For each entry a
 *     eu.dnetlib.dhp.Project entity is created and the information about H2020Classification is set together with the
 *     h2020topiccode variable
 *  2. join between the output of the previous step and the topic information on the topic code. Each time a match is
 *     found the h2020topicdescription variable is set.
 *
 * To produce one single entry for each project code a step of groupoing is needed: each project can be associated to more
 * than one programme.
 */
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

		final String topicPath = parser.get("topicPath");
		log.info("topic path {}: ", topicPath);

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
					topicPath,
					outputPath);
			});
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	private static void getAtomicActions(SparkSession spark, String projectPatH,
		String programmePath,
		String topicPath,
		String outputPath) {

		Dataset<CSVProject> project = readPath(spark, projectPatH, CSVProject.class);
		Dataset<CSVProgramme> programme = readPath(spark, programmePath, CSVProgramme.class);
		Dataset<JsonTopic> topic = readPath(spark, topicPath, JsonTopic.class);

		Dataset<Project> aaproject = project
			.joinWith(programme, project.col("programme").equalTo(programme.col("code")), "left")
			.map((MapFunction<Tuple2<CSVProject, CSVProgramme>, Project>) c -> {

				CSVProject csvProject = c._1();

				return Optional
					.ofNullable(c._2())
					.map(csvProgramme -> {
						Project pp = new Project();
						pp
							.setId(
								csvProject.getId());
						pp.setH2020topiccode(csvProject.getTopics());
						H2020Programme pm = new H2020Programme();
						H2020Classification h2020classification = new H2020Classification();
						pm.setCode(csvProject.getProgramme());
						h2020classification.setClassification(csvProgramme.getClassification());
						h2020classification.setH2020Programme(pm);
						setLevelsandProgramme(h2020classification, csvProgramme.getClassification_short());
						pp.setH2020classification(Arrays.asList(h2020classification));

						return pp;
					})
					.orElse(null);

			}, Encoders.bean(Project.class))
			.filter(Objects::nonNull);

		aaproject
			.joinWith(topic, aaproject.col("id").equalTo(topic.col("projectID")), "left")
			.map((MapFunction<Tuple2<Project, JsonTopic>, Project>) p -> {
				Optional<JsonTopic> op = Optional.ofNullable(p._2());
				Project rp = p._1();
				rp
					.setId(
						createOpenaireId(
							ModelSupport.entityIdPrefix.get("project"),
							"corda__h2020", rp.getId()));
				op.ifPresent(excelTopic -> rp.setH2020topicdescription(excelTopic.getTitle()));
				return rp;
			}, Encoders.bean(Project.class))
			.filter(Objects::nonNull)
			.groupByKey(
				(MapFunction<Project, String>) OafEntity::getId,
				Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, Project, Project>) (s, it) -> {
				Project first = it.next();
				it.forEachRemaining(first::mergeFrom);
				return first;
			}, Encoders.bean(Project.class))
			.toJavaRDD()
			.map(p -> new AtomicAction(Project.class, p))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class);

	}

	private static void setLevelsandProgramme(H2020Classification h2020Classification, String classification_short) {
		String[] tmp = classification_short.split(" \\| ");
		h2020Classification.setLevel1(tmp[0]);
		if (tmp.length > 1) {
			h2020Classification.setLevel2(tmp[1]);
		}
		if (tmp.length > 2) {
			h2020Classification.setLevel3(tmp[2]);
		}
		h2020Classification.getH2020Programme().setDescription(tmp[tmp.length - 1]);
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
