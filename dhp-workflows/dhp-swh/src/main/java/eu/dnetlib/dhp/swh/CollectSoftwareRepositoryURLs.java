
package eu.dnetlib.dhp.swh;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Result;

/**
 *  Collects unique software repository URLs in the Graph using Hive
 *
 * @author Serafeim Chatzopoulos
 */
public class CollectSoftwareRepositoryURLs implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(CollectSoftwareRepositoryURLs.class);

	public static <I extends Result> void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				CollectSoftwareRepositoryURLs.class
					.getResourceAsStream("/eu/dnetlib/dhp/swh/input_collect_software_repository_urls.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String hiveDbName = parser.get("hiveDbName");
		log.info("hiveDbName {}: ", hiveDbName);

		final String outputPath = parser.get("softwareCodeRepositoryURLs");
		log.info("softwareCodeRepositoryURLs {}: ", outputPath);

		final String hiveMetastoreUris = parser.get("hiveMetastoreUris");
		log.info("hiveMetastoreUris: {}", hiveMetastoreUris);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", hiveMetastoreUris);

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				doRun(spark, hiveDbName, outputPath);
			});
	}

	private static <I extends Result> void doRun(SparkSession spark, String hiveDbName, String outputPath) {

		String queryTemplate = "SELECT distinct coderepositoryurl.value " +
			"FROM %s.software " +
			"WHERE coderepositoryurl.value IS NOT NULL " +
			"AND datainfo.deletedbyinference = FALSE " +
			"AND datainfo.invisible = FALSE " +
			"LIMIT 1000";
		String query = String.format(queryTemplate, hiveDbName);

		log.info("Hive query to fetch software code URLs: {}", query);

		Dataset<Row> df = spark.sql(query);

		// write distinct repository URLs
		df
			.write()
			.mode(SaveMode.Overwrite)
			.csv(outputPath);
	}
}
