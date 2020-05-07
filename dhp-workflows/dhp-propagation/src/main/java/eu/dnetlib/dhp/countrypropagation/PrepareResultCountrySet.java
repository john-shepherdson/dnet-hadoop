
package eu.dnetlib.dhp.countrypropagation;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;

public class PrepareResultCountrySet {
	private static final Logger log = LoggerFactory.getLogger(PrepareResultCountrySet.class);

	private static final String RESULT_COUNTRYSET_QUERY = "SELECT id resultId, collect_set(country) countrySet "
		+ "FROM ( SELECT id, country "
		+ "FROM datasource_country JOIN cfhb ON cf = dataSourceId "
		+ "UNION ALL "
		+ "SELECT id, country FROM datasource_country "
		+ "JOIN cfhb ON hb = dataSourceId ) tmp "
		+ "GROUP BY id";

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				PrepareResultCountrySet.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/countrypropagation/input_prepareresultcountry_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String datasourcecountrypath = parser.get("preparedInfoPath");
		log.info("preparedInfoPath: {}", datasourcecountrypath);

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				getPotentialResultToUpdate(
					spark,
					inputPath,
					outputPath,
					datasourcecountrypath,
					resultClazz);
			});
	}

	private static <R extends Result> void getPotentialResultToUpdate(
		SparkSession spark,
		String inputPath,
		String outputPath,
		String datasourcecountrypath,
		Class<R> resultClazz) {

		Dataset<R> result = readPath(spark, inputPath, resultClazz);
		result.createOrReplaceTempView("result");
		// log.info("number of results: {}", result.count());
		createCfHbforResult(spark);

		Dataset<DatasourceCountry> datasource_country = readPath(spark, datasourcecountrypath, DatasourceCountry.class);

		datasource_country.createOrReplaceTempView("datasource_country");
		// log.info("datasource_country number : {}", datasource_country.count());

		spark
			.sql(RESULT_COUNTRYSET_QUERY)
			.as(Encoders.bean(ResultCountrySet.class))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Append)
			.json(outputPath);
	}

}
