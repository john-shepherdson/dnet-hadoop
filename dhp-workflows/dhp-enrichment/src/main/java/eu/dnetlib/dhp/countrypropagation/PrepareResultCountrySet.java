
package eu.dnetlib.dhp.countrypropagation;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

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
				removeOutputDir(spark, outputPath);
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

		createCfHbforResult(spark);

		Dataset<DatasourceCountry> datasource_country = readPath(spark, datasourcecountrypath, DatasourceCountry.class);

		datasource_country.createOrReplaceTempView("datasource_country");

		spark
			.sql(RESULT_COUNTRYSET_QUERY)
			.as(Encoders.bean(ResultCountrySet.class))
			.toJavaRDD()
			.mapToPair(value -> new Tuple2<>(value.getResultId(), value))
			.reduceByKey((a, b) -> {
				ArrayList<CountrySbs> countryList = a.getCountrySet();
				Set<String> countryCodes = countryList
					.stream()
					.map(CountrySbs::getClassid)
					.collect(Collectors.toSet());
				b
					.getCountrySet()
					.stream()
					.forEach(c -> {
						if (!countryCodes.contains(c.getClassid())) {
							countryList.add(c);
							countryCodes.add(c.getClassid());
						}

					});
				a.setCountrySet(countryList);
				return a;
			})
			.map(couple -> OBJECT_MAPPER.writeValueAsString(couple._2()))
			.saveAsTextFile(outputPath, GzipCodec.class);
	}

}
