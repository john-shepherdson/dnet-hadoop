
package eu.dnetlib.dhp.actionmanager.stats_actionsets;

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
public class StatsAtomicActionsJob implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(StatsAtomicActionsJob.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static <I extends Result> void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				StatsAtomicActionsJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/stats_actionsets/input_actionset_parameter.json"));

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

		final String dbname = parser.get("statsDB");

		final String workingPath = parser.get("workingPath");

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareGreenData(dbname, spark, workingPath + "/greenOADB", "indi_pub_green_oa", "id");
				prepareDiamondData(dbname, spark, workingPath + "/diamondOADΒ", "indi_pub_diamond", "id");
				preparePubliclyFundedData(
					dbname, spark, workingPath + "/publiclyFundedDΒ", "indi_funded_result_with_fundref", "id");
				prepareOAColourData(dbname, spark, workingPath + "/oacolourDB", "", "id");
				writeActionSet(spark, workingPath, outputPath);
			});
	}

	private static void prepareGreenData(String dbname, SparkSession spark, String workingPath, String tableName,
		String resultAttributeName) {
		spark
			.sql(
				String
					.format(
						"select %s as id, green_oa as green_oa " +
							"from %s.%s",
						resultAttributeName, dbname, tableName))
			.as(Encoders.bean(StatsGreenOAModel.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath);
	}

	private static void prepareDiamondData(String dbname, SparkSession spark, String workingPath, String tableName,
		String resultAttributeName) {
		spark
			.sql(
				String
					.format(
						"select %s as id, in_diamond_journal as in_diamond_journal " +
							"from %s.%s",
						resultAttributeName, dbname, tableName))
			.as(Encoders.bean(StatsDiamondOAModel.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath);
	}

	private static void preparePubliclyFundedData(String dbname, SparkSession spark, String workingPath,
		String tableName,
		String resultAttributeName) {
		spark
			.sql(
				String
					.format(
						"select %s as id, fundref as publicly_funded " +
							"from %s.%s",
						resultAttributeName, dbname, tableName))
			.as(Encoders.bean(StatsPubliclyFundedModel.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath);
	}

	private static void prepareOAColourData(String dbname, SparkSession spark, String workingPath, String tableName,
		String resultAttributeName) {
		spark
			.sql(
				String
					.format(
						"select b.%s as id, is_gold, is_bronze_oa, is_hybrid  from %s.indi_pub_bronze_oa b " +
							"left outer join %s.indi_pub_gold_oa g on g.id=b.id " +
							"left outer join %s.indi_pub_hybrid h on b.id=h.id",
						resultAttributeName, dbname, dbname, dbname))
			.as(Encoders.bean(StatsOAColourModel.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath);
	}

	public static void writeActionSet(SparkSession spark, String inputPath, String outputPath) {

		getFinalIndicatorsGreenResult(spark, inputPath + "/greenOADB")
			.toJavaRDD()
			.map(p -> new AtomicAction(p.getClass(), p))
			.union(
				getFinalIndicatorsDiamondResult(spark, inputPath + "/diamondOADΒ")
					.toJavaRDD()
					.map(p -> new AtomicAction(p.getClass(), p)))
			.union(
				getFinalIndicatorsPubliclyFundedResult(spark, inputPath + "/publiclyFundedDΒ")
					.toJavaRDD()
					.map(p -> new AtomicAction(p.getClass(), p)))
			.union(
				getFinalIndicatorsOAColourResult(spark, inputPath + "/oacolourDB")
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

	private static Dataset<Result> getFinalIndicatorsGreenResult(SparkSession spark, String inputPath) {

		return readPath(spark, inputPath, StatsGreenOAModel.class)
			.map((MapFunction<StatsGreenOAModel, Result>) usm -> {
				Result r = new Result();
				r.setId("50|" + usm.getId());
				r.setMeasures(getMeasure(usm.isGreen_oa(), "green_oa"));
				return r;
			}, Encoders.bean(Result.class));
	}

	private static Dataset<Result> getFinalIndicatorsDiamondResult(SparkSession spark, String inputPath) {

		return readPath(spark, inputPath, StatsDiamondOAModel.class)
			.map((MapFunction<StatsDiamondOAModel, Result>) usm -> {
				Result r = new Result();
				r.setId("50|" + usm.getId());
				r.setMeasures(getMeasure(usm.isIn_diamond_journal(), "in_diamond_journal"));
				return r;
			}, Encoders.bean(Result.class));
	}

	private static Dataset<Result> getFinalIndicatorsPubliclyFundedResult(SparkSession spark, String inputPath) {

		return readPath(spark, inputPath, StatsPubliclyFundedModel.class)
			.map((MapFunction<StatsPubliclyFundedModel, Result>) usm -> {
				Result r = new Result();
				r.setId("50|" + usm.getId());
				r.setMeasures(getMeasure(usm.isPublicly_funded(), "publicly_funded"));
				return r;
			}, Encoders.bean(Result.class));
	}

	private static Dataset<Result> getFinalIndicatorsOAColourResult(SparkSession spark, String inputPath) {

		return readPath(spark, inputPath, StatsOAColourModel.class)
			.map((MapFunction<StatsOAColourModel, Result>) usm -> {
				Result r = new Result();
				r.setId("50|" + usm.getId());
				r.setMeasures(getMeasureOAColour(usm.isIs_gold(), usm.isIs_bronze_oa(), usm.isIs_hybrid()));
				return r;
			}, Encoders.bean(Result.class));
	}

	private static List<Measure> getMeasure(Boolean is_model_oa, String model_type) {
		DataInfo dataInfo = OafMapperUtils
			.dataInfo(
				false,
				UPDATE_DATA_INFO_TYPE,
				true,
				false,
				OafMapperUtils
					.qualifier(
						UPDATE_MEASURE_STATS_MODEL_CLASS_ID,
						UPDATE_CLASS_NAME,
						ModelConstants.DNET_PROVENANCE_ACTIONS,
						ModelConstants.DNET_PROVENANCE_ACTIONS),
				"");

		return Arrays
			.asList(
				OafMapperUtils
					.newMeasureInstance(model_type, String.valueOf(is_model_oa), UPDATE_KEY_STATS_MODEL, dataInfo));
	}

	private static List<Measure> getMeasureOAColour(Boolean is_gold, Boolean is_bronze_oa, Boolean is_hybrid) {
		DataInfo dataInfo = OafMapperUtils
			.dataInfo(
				false,
				UPDATE_DATA_INFO_TYPE,
				true,
				false,
				OafMapperUtils
					.qualifier(
						UPDATE_MEASURE_STATS_MODEL_CLASS_ID,
						UPDATE_CLASS_NAME,
						ModelConstants.DNET_PROVENANCE_ACTIONS,
						ModelConstants.DNET_PROVENANCE_ACTIONS),
				"");

		return Arrays
			.asList(
				OafMapperUtils
					.newMeasureInstance("is_gold", String.valueOf(is_gold), UPDATE_KEY_STATS_MODEL, dataInfo),
				OafMapperUtils
					.newMeasureInstance("is_bronze_oa", String.valueOf(is_bronze_oa), UPDATE_KEY_STATS_MODEL, dataInfo),
				OafMapperUtils
					.newMeasureInstance("is_hybrid", String.valueOf(is_hybrid), UPDATE_KEY_STATS_MODEL, dataInfo));

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
