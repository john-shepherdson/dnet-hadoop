
package eu.dnetlib.dhp.oa.graph.hive;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Oaf;

public class GraphHiveTableExporterJob {

	private static final Logger log = LoggerFactory.getLogger(GraphHiveTableExporterJob.class);

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GraphHiveTableExporterJob.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/graph/hive_db_exporter_parameters.json")));
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		int numPartitions = Optional
			.ofNullable(parser.get("numPartitions"))
			.map(Integer::valueOf)
			.orElse(-1);
		log.info("numPartitions: {}", numPartitions);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		String hiveTableName = parser.get("hiveTableName");
		log.info("hiveTableName: {}", hiveTableName);

		String hiveMetastoreUris = parser.get("hiveMetastoreUris");
		log.info("hiveMetastoreUris: {}", hiveMetastoreUris);

		String mode = parser.get("mode");
		log.info("mode: {}", mode);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", hiveMetastoreUris);

		runWithSparkHiveSession(
			conf, isSparkSessionManaged,
			spark -> saveGraphTable(spark, outputPath, hiveTableName, mode, numPartitions));
	}

	// protected for testing
	private static <T extends Oaf> void saveGraphTable(SparkSession spark, String outputPath, String hiveTableName,
		String mode, int numPartitions) {

		Dataset<Row> dataset = spark.table(hiveTableName);

		if (numPartitions > 0) {
			log.info("repartitioning to {} partitions", numPartitions);
			dataset = dataset.repartition(numPartitions);
		}

		dataset
			.write()
			.mode(mode)
			.option("compression", "gzip")
			.json(outputPath);
	}
}
