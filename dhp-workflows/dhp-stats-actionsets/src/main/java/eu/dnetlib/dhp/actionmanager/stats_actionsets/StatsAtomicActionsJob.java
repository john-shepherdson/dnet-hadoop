
package eu.dnetlib.dhp.actionmanager.stats_actionsets;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
		conf.set("spark.speculation", "false");
		conf.set("spark.hadoop.mapreduce.map.speculative", "false");
		conf.set("spark.hadoop.mapreduce.reduce.speculative", "false");

		final String dbname = parser.get("statsDB");

		final String workingPath = parser.get("workingPath");

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				prepareResultEnhancement(dbname, spark, workingPath + "/resultEnhancements", "id");
				writeActionSet(spark, workingPath, outputPath);
			});
	}

	private static void prepareResultEnhancement(String dbname, SparkSession spark, String workingPath,
		String resultAttributeName) {
		spark
			.sql(
				String
					.format(
						"select b.%s as id, is_gold, is_bronze_oa, is_hybrid,green_oa, in_diamond_journal,f.fundref as publicly_funded "
							+ "from %s.indi_pub_bronze_oa b " +
							"left outer join %s.indi_pub_gold_oa g on g.id=b.id " +
							"left outer join %s.indi_pub_hybrid h on b.id=h.id " +
							"left outer join %s.indi_pub_green_oa gr on b.id=gr.id " +
							"left outer join %s.indi_pub_diamond d on b.id=d.id " +
							"left outer join %s.indi_funded_result_with_fundref f on b.id=f.id ",
						resultAttributeName, dbname, dbname, dbname, dbname, dbname, dbname))
			.as(Encoders.bean(StatsResultEnhancementModel.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath);
	}

	public static void writeActionSet(SparkSession spark, String inputPath, String outputPath) {

		getResultEnhancements(spark, inputPath + "/resultEnhancements")
			.toJavaRDD()
			.map(p -> new AtomicAction(p.getClass(), p))
			.mapToPair(
				aa -> new Tuple2<>(new Text(aa.getClazz().getCanonicalName()),
					new Text(OBJECT_MAPPER.writeValueAsString(aa))))
			.saveAsHadoopFile(
				outputPath,
				Text.class,
				Text.class,
				SequenceFileOutputFormat.class,
				GzipCodec.class);
	}

	private static Dataset<Result> getResultEnhancements(SparkSession spark, String inputPath) {

		return readPath(spark, inputPath, StatsResultEnhancementModel.class)
			.map((MapFunction<StatsResultEnhancementModel, Result>) usm -> {
				Result r = new Result();
				r.setId("50|" + usm.getId());
				r.setIsInDiamondJournal(usm.isIn_diamond_journal());
				r.setIsGreen(usm.isGreen_oa());
				r.setPubliclyFunded(usm.isPublicly_funded());
				if (usm.isIs_bronze_oa())
					r.setOpenAccessColor(OpenAccessColor.bronze);
				else if (usm.isIs_gold())
					r.setOpenAccessColor(OpenAccessColor.bronze);
				else if (usm.isIs_gold())
					r.setOpenAccessColor(OpenAccessColor.gold);
				return r;
			}, Encoders.bean(Result.class));
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
