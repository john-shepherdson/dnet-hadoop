
package eu.dnetlib.dhp.actionmanager.partition;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.actionmanager.ISClient;
import eu.dnetlib.dhp.actionmanager.promote.PromoteActionPayloadForGraphTableJob;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;

/** Partitions given set of action sets by payload type. */
public class PartitionActionSetsByPayloadTypeJob {

	private static final Logger logger = LoggerFactory.getLogger(PartitionActionSetsByPayloadTypeJob.class);

	private static final StructType KV_SCHEMA = StructType$.MODULE$
		.apply(
			Arrays
				.asList(
					StructField$.MODULE$.apply("key", DataTypes.StringType, false, Metadata.empty()),
					StructField$.MODULE$.apply("value", DataTypes.StringType, false, Metadata.empty())));

	private static final StructType ATOMIC_ACTION_SCHEMA = StructType$.MODULE$
		.apply(
			Arrays
				.asList(
					StructField$.MODULE$.apply("clazz", DataTypes.StringType, false, Metadata.empty()),
					StructField$.MODULE$
						.apply(
							"payload", DataTypes.StringType, false, Metadata.empty())));

	private ISClient isClient;

	public PartitionActionSetsByPayloadTypeJob(String isLookupUrl) {
		this.isClient = new ISClient(isLookupUrl);
	}

	public PartitionActionSetsByPayloadTypeJob() {
	}

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				PromoteActionPayloadForGraphTableJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/partition/partition_action_sets_by_payload_type_input_parameters.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		logger.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputActionSetIds = parser.get("inputActionSetIds");
		logger.info("inputActionSetIds: {}", inputActionSetIds);

		String outputPath = parser.get("outputPath");
		logger.info("outputPath: {}", outputPath);

		String isLookupUrl = parser.get("isLookupUrl");
		logger.info("isLookupUrl: {}", isLookupUrl);

		new PartitionActionSetsByPayloadTypeJob(isLookupUrl)
			.run(isSparkSessionManaged, inputActionSetIds, outputPath);
	}

	protected void run(Boolean isSparkSessionManaged, String inputActionSetIds, String outputPath) {

		List<String> inputActionSetPaths = getIsClient().getLatestRawsetPaths(inputActionSetIds);
		logger.info("inputActionSetPaths: {}", String.join(",", inputActionSetPaths));

		SparkConf conf = new SparkConf();
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				readAndWriteActionSetsFromPaths(spark, inputActionSetPaths, outputPath);
			});
	}

	private static void removeOutputDir(SparkSession spark, String path) {
		HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
	}

	private static void readAndWriteActionSetsFromPaths(
		SparkSession spark, List<String> inputActionSetPaths, String outputPath) {
		inputActionSetPaths
			.stream()
			.filter(path -> HdfsSupport.exists(path, spark.sparkContext().hadoopConfiguration()))
			.forEach(
				inputActionSetPath -> {
					Dataset<Row> actionDS = readActionSetFromPath(spark, inputActionSetPath);
					saveActions(actionDS, outputPath);
				});
	}

	private static Dataset<Row> readActionSetFromPath(SparkSession spark, String path) {
		logger.info("Reading actions from path: {}", path);

		JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<Row> rdd = sc
			.sequenceFile(path, Text.class, Text.class)
			.map(x -> RowFactory.create(x._1().toString(), x._2().toString()));

		return spark
			.createDataFrame(rdd, KV_SCHEMA)
			.withColumn("atomic_action", from_json(col("value"), ATOMIC_ACTION_SCHEMA))
			.select(expr("atomic_action.*"));
	}

	private static void saveActions(Dataset<Row> actionDS, String path) {
		logger.info("Saving actions to path: {}", path);
		actionDS.write().partitionBy("clazz").mode(SaveMode.Append).parquet(path);
	}

	public ISClient getIsClient() {
		return isClient;
	}

	public void setIsClient(ISClient isClient) {
		this.isClient = isClient;
	}
}
