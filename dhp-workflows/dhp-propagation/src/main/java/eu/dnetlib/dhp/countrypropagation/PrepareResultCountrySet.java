
package eu.dnetlib.dhp.countrypropagation;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;

public class PrepareResultCountrySet {
	private static final Logger log = LoggerFactory.getLogger(PrepareResultCountrySet.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkCountryPropagationJob2.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/countrypropagation/input_prepareresultcountry_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

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
					datasourcecountrypath,
					resultClazz);
			});

	}

	private static <R extends Result> void getPotentialResultToUpdate(
		SparkSession spark,
		String inputPath,
		String datasourcecountrypath,
		Class<R> resultClazz) {

		Dataset<R> result = readPathEntity(spark, inputPath, resultClazz);
		result.createOrReplaceTempView("result");
		// log.info("number of results: {}", result.count());
		createCfHbforresult(spark);
		Dataset<DatasourceCountry> datasourcecountryassoc = readAssocDatasourceCountry(spark, datasourcecountrypath);
		countryPropagationAssoc(spark, datasourcecountryassoc)
			.map((MapFunction<ResultCountrySet, R>) value -> {
				R ret = resultClazz.newInstance();
				ret.setId(value.getResultId());
				ret
					.setCountry(
						value
							.getCountrySet()
							.stream()
							.map(c -> getCountry(c.getClassid(), c.getClassname()))
							.collect(Collectors.toList()));
				return ret;
			}, Encoders.bean(resultClazz))
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Append)
			.json(inputPath);
	}

	private static Dataset<ResultCountrySet> countryPropagationAssoc(
		SparkSession spark,
		Dataset<DatasourceCountry> datasource_country) {

		// Dataset<DatasourceCountry> datasource_country = broadcast_datasourcecountryassoc.value();
		datasource_country.createOrReplaceTempView("datasource_country");
		log.info("datasource_country number : {}", datasource_country.count());

		String query = "SELECT id resultId, collect_set(country) countrySet "
			+ "FROM ( SELECT id, country "
			+ "FROM datasource_country "
			+ "JOIN cfhb "
			+ " ON cf = dataSourceId     "
			+ "UNION ALL "
			+ "SELECT id , country     "
			+ "FROM datasource_country "
			+ "JOIN cfhb "
			+ " ON hb = dataSourceId   ) tmp "
			+ "GROUP BY id";
		Dataset<ResultCountrySet> potentialUpdates = spark
			.sql(query)
			.as(Encoders.bean(ResultCountrySet.class));
		// log.info("potential update number : {}", potentialUpdates.count());
		return potentialUpdates;
	}

	private static Dataset<DatasourceCountry> readAssocDatasourceCountry(
		SparkSession spark, String relationPath) {
		return spark
			.read()
			.textFile(relationPath)
			.map(
				value -> OBJECT_MAPPER.readValue(value, DatasourceCountry.class),
				Encoders.bean(DatasourceCountry.class));
	}
}
