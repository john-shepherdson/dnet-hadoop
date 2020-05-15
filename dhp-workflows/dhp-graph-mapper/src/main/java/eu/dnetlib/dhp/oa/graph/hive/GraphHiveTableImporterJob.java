
package eu.dnetlib.dhp.oa.graph.hive;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;
import static eu.dnetlib.dhp.schema.common.ModelSupport.tableIdentifier;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Oaf;

public class GraphHiveTableImporterJob {

	private static final Logger log = LoggerFactory.getLogger(GraphHiveTableImporterJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					GraphHiveTableImporterJob.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/oa/graph/hive_table_importer_parameters.json")));
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		String hiveDbName = parser.get("hiveDbName");
		log.info("hiveDbName: {}", hiveDbName);

		final String className = parser.get("className");
		log.info("className: {}", className);

		Class<? extends Oaf> clazz = (Class<? extends Oaf>) Class.forName(className);

		String hiveMetastoreUris = parser.get("hiveMetastoreUris");
		log.info("hiveMetastoreUris: {}", hiveMetastoreUris);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", hiveMetastoreUris);

		runWithSparkHiveSession(
			conf, isSparkSessionManaged, spark -> loadGraphTable(spark, inputPath, hiveDbName, clazz));
	}

	// protected for testing
	private static <T extends Oaf> void loadGraphTable(SparkSession spark, String inputPath, String hiveDbName,
		Class<T> clazz) {

		spark
			.read()
			.textFile(inputPath)
			.map((MapFunction<String, T>) s -> OBJECT_MAPPER.readValue(s, clazz), Encoders.bean(clazz))
			.write()
			.mode(SaveMode.Overwrite)
			.saveAsTable(tableIdentifier(hiveDbName, clazz));
	}

}
