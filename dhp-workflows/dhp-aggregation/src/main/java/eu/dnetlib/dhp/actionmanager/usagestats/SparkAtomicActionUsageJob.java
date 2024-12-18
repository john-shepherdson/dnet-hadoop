
package eu.dnetlib.dhp.actionmanager.usagestats;

import static eu.dnetlib.dhp.actionmanager.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
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

		final String datasourcePath = parser.get("datasourcePath");

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareResultData(
					dbname, spark, workingPath + "/usageDb",
					"usage_stats",
					"result_id",
					"repository_id",
					datasourcePath);
				prepareData(dbname, spark, workingPath + "/projectDb", "project_stats", "id");
				prepareData(dbname, spark, workingPath + "/datasourceDb", "datasource_stats", "repository_id");
				writeActionSet(spark, workingPath, outputPath);
			});
	}

	private static void prepareResultData(String dbname, SparkSession spark, String workingPath, String tableName,
		String resultAttributeName, String datasourceAttributeName,
		String datasourcePath) {
		Dataset<UsageStatsResultModel> resultModel = spark
			.sql(
				String
					.format(
						"select %s as id, %s as datasourceId, sum(downloads) as downloads, sum(views) as views " +
							"from %s.%s group by %s, %s",
						resultAttributeName, datasourceAttributeName, dbname, tableName, resultAttributeName,
						datasourceAttributeName))
			.as(Encoders.bean(UsageStatsResultModel.class));
		Dataset<Datasource> datasource = readPath(spark, datasourcePath, Datasource.class)
			.filter((FilterFunction<Datasource>) d -> !d.getDataInfo().getDeletedbyinference())
			.map((MapFunction<Datasource, Datasource>) d -> {
				d.setId(d.getId().substring(3));
				return d;
			}, Encoders.bean(Datasource.class));
		resultModel
			.joinWith(datasource, resultModel.col("datasourceId").equalTo(datasource.col("id")), "left")
			.map((MapFunction<Tuple2<UsageStatsResultModel, Datasource>, UsageStatsResultModel>) t2 -> {
				UsageStatsResultModel usrm = t2._1();
				if (Optional.ofNullable(t2._2()).isPresent())
					usrm.setDatasourceId(usrm.getDatasourceId() + "||" + t2._2().getOfficialname().getValue());
				else
					usrm.setDatasourceId(usrm.getDatasourceId() + "||NO_MATCH_FOUND");
				return usrm;
			}, Encoders.bean(UsageStatsResultModel.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath);
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

	public static Measure newMeasureInstance(String id) {
		Measure m = new Measure();
		m.setId(id);
		m.setUnit(new ArrayList<>());
		return m;
	}

	private static Dataset<Result> getFinalIndicatorsResult(SparkSession spark, String inputPath) {

		return readPath(spark, inputPath, UsageStatsResultModel.class)
			.groupByKey((MapFunction<UsageStatsResultModel, String>) usm -> usm.getId(), Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, UsageStatsResultModel, Result>) (k, it) -> {
				Result r = new Result();
				r.setId("50|" + k);
				// id = download or view and unit = list of key value pairs
				Measure download = newMeasureInstance("downloads");
				Measure view = newMeasureInstance("views");
				UsageStatsResultModel first = it.next();
				addCountForDatasource(download, first, view);
				it.forEachRemaining(usm -> {
					addCountForDatasource(download, usm, view);
				});
				r.setMeasures(Arrays.asList(download, view));
				return r;
			}, Encoders.bean(Result.class))
//			.map((MapFunction<UsageStatsResultModel, Result>) usm -> {
//				Result r = new Result();
//				r.setId("50|" + usm.getId());
//				r.setMeasures(getMeasure(usm.getDownloads(), usm.getViews()));
//				return r;
//			}, Encoders.bean(Result.class));
		;
	}

	private static void addCountForDatasource(Measure download, UsageStatsResultModel usm, Measure view) {
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
		download
			.getUnit()
			.add(
				OafMapperUtils
					.newKeyValueInstance(usm.getDatasourceId(), String.valueOf(usm.getDownloads()), dataInfo));
		view
			.getUnit()
			.add(OafMapperUtils.newKeyValueInstance(usm.getDatasourceId(), String.valueOf(usm.getViews()), dataInfo));
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
