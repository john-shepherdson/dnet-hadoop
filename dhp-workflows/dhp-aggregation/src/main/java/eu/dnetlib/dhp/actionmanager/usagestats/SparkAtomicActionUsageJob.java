
package eu.dnetlib.dhp.actionmanager.usagestats;

import static eu.dnetlib.dhp.actionmanager.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;
import scala.Tuple2;

/**
 * created the Atomic Action for each type of results
 */
public class SparkAtomicActionUsageJob implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkAtomicActionUsageJob.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static <I extends Result> void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkAtomicActionUsageJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/usagestats/input_actionset_parameter.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

		final String dbname = parser.get("usagestatsdb");

		final String workingPath = parser.get("workingPath");

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareData(dbname, spark, workingPath + "/usageDb", "usage_stats", "result_id");
				prepareData(dbname, spark, workingPath + "/projectDb", "project_stats", "id");
				prepareData(dbname, spark, workingPath + "/datasourceDb", "datasource_stats", "repository_id");
				writeActionSet(spark, workingPath, outputPath);
			});
	}

	private static void prepareData(String dbname, SparkSession spark, String workingPath, String tableName,
		String attribute_name) {
		spark
			.sql(
				String
					.format(
						"select %s as id, sum(downloads) as downloads, sum(views) as views " +
							"from %s.%s group by %s",
						attribute_name, dbname, tableName, attribute_name))
			.as(Encoders.bean(UsageStatsModel.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath);
	}

	public static void writeActionSet(SparkSession spark, String inputPath, String outputPath) {
		getFinalIndicatorsResult(spark, inputPath + "/usageDb")
			.toJavaRDD()
			.map(p -> new AtomicAction(p.getClass(), p))
			.union(
				getFinalIndicatorsProject(spark, inputPath + "/projectDb")
					.toJavaRDD()
					.map(p -> new AtomicAction(p.getClass(), p)))
			.union(
				getFinalIndicatorsDatasource(spark, inputPath + "/datasourceDb")
					.toJavaRDD()
					.map(p -> new AtomicAction(p.getClass(), p)))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(outputPath, Text.class, Text.class, SequenceFileOutputFormat.class);

	}

	private static Dataset<Result> getFinalIndicatorsResult(SparkSession spark, String inputPath) {

		return readPath(spark, inputPath, UsageStatsModel.class)
			.map((MapFunction<UsageStatsModel, Result>) usm -> {
				Result r = new Result();
				r.setId("50|" + usm.getId());
				r.setMeasures(getMeasure(usm.getDownloads(), usm.getViews()));
				return r;
			}, Encoders.bean(Result.class));
	}

	private static Dataset<Project> getFinalIndicatorsProject(SparkSession spark, String inputPath) {

		return readPath(spark, inputPath, UsageStatsModel.class)
			.map((MapFunction<UsageStatsModel, Project>) usm -> {
				Project p = new Project();
				p.setId("40|" + usm.getId());
				p.setMeasures(getMeasure(usm.getDownloads(), usm.getViews()));
				return p;
			}, Encoders.bean(Project.class));
	}

	private static Dataset<Datasource> getFinalIndicatorsDatasource(SparkSession spark, String inputPath) {

		return readPath(spark, inputPath, UsageStatsModel.class)
			.map((MapFunction<UsageStatsModel, Datasource>) usm -> {
				Datasource d = new Datasource();
				d.setId("10|" + usm.getId());
				d.setMeasures(getMeasure(usm.getDownloads(), usm.getViews()));
				return d;
			}, Encoders.bean(Datasource.class));
	}

	private static List<Measure> getMeasure(Long downloads, Long views) {
		DataInfo dataInfo = OafMapperUtils
			.dataInfo(
				false,
				UPDATE_DATA_INFO_TYPE,
				true,
				false,
				OafMapperUtils
					.qualifier(
						UPDATE_MEASURE_USAGE_COUNTS_CLASS_ID,
						UPDATE_CLASS_NAME,
						ModelConstants.DNET_PROVENANCE_ACTIONS,
						ModelConstants.DNET_PROVENANCE_ACTIONS),
				"");

		return Arrays
			.asList(
				OafMapperUtils
					.newMeasureInstance("downloads", String.valueOf(downloads), UPDATE_KEY_USAGE_COUNTS, dataInfo),
				OafMapperUtils.newMeasureInstance("views", String.valueOf(views), UPDATE_KEY_USAGE_COUNTS, dataInfo));

	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	public static <R> Dataset<R> readPath(
		SparkSession spark, String inputPath, Class<R> clazz) {
		return spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, R>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz));
	}

}
