
package eu.dnetlib.dhp.actionmanager.usagestats;

import static eu.dnetlib.dhp.actionmanager.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
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
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Measure;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils;

/**
 * created the Atomic Action for each tipe of results
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
				prepareResults(dbname, spark, workingPath);
				prepareActionSet(spark, workingPath, outputPath);
			});
	}

	public static void prepareResults(String db, SparkSession spark, String workingPath) {
		spark
			.sql(
				"Select result_id, downloads, views " +
					"from " + db + ".usage_stats")
			.as(Encoders.bean(UsageStatsModel.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingPath);
	}

	public static void prepareActionSet(SparkSession spark, String inputPath, String outputPath) {
		readPath(spark, inputPath, UsageStatsModel.class)
			.groupByKey((MapFunction<UsageStatsModel, String>) us -> us.getResult_id(), Encoders.STRING())
			.mapGroups((MapGroupsFunction<String, UsageStatsModel, Result>) (k, it) -> {
				UsageStatsModel first = it.next();
				it.forEachRemaining(us -> {
					first.setDownloads(first.getDownloads() + us.getDownloads());
					first.setViews(first.getViews() + us.getViews());
				});

				Result res = new Result();
				res.setId("50|" + k);

				res.setMeasures(getMeasure(first.getDownloads(), first.getViews()));
				return res;
			}, Encoders.bean(Result.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
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
