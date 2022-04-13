
package eu.dnetlib.dhp.countrypropagation;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.EntityEntityRel;
import eu.dnetlib.dhp.PropagationConstant;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

public class PrepareResultCountrySet {
	private static final Logger log = LoggerFactory.getLogger(PrepareResultCountrySet.class);

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

		String workingPath = parser.get("workingPath");

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		final String resultType = resultClassName.substring(resultClassName.lastIndexOf(".") + 1).toLowerCase();
		log.info("resultType: {}", resultType);

		String outputPath = workingPath + "/" + resultType; // parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String datasourcecountrypath = workingPath + "/datasourceCountry";// parser.get("preparedInfoPath");
		log.info("preparedInfoPath: {}", datasourcecountrypath);

		Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				getPotentialResultToUpdate(
					spark,
					inputPath,
					outputPath,
					datasourcecountrypath,
					workingPath + "/resultCfHb/" + resultType,
					resultClazz);
			});
	}

	private static <R extends Result> void getPotentialResultToUpdate(
		SparkSession spark,
		String inputPath,
		String outputPath,
		String datasourcecountrypath,
		String workingPath,
		Class<R> resultClazz) {

		PropagationConstant.createCfHbforResult(spark, inputPath, workingPath, resultClazz);

		Dataset<DatasourceCountry> datasource_country = readPath(spark, datasourcecountrypath, DatasourceCountry.class);

		Dataset<EntityEntityRel> cfhb = readPath(spark, workingPath, EntityEntityRel.class);

		datasource_country
			.joinWith(
				cfhb, cfhb
					.col("entity2Id")
					.equalTo(datasource_country.col("datasourceId")))
			.groupByKey(
				(MapFunction<Tuple2<DatasourceCountry, EntityEntityRel>, String>) t2 -> t2._2().getEntity1Id(),
				Encoders.STRING())
			.mapGroups(
				(MapGroupsFunction<String, Tuple2<DatasourceCountry, EntityEntityRel>, ResultCountrySet>) (k, it) -> {
					ResultCountrySet rcs = new ResultCountrySet();
					rcs.setResultId(k);
					Set<CountrySbs> set = new HashSet<>();
					Set<String> countryCodes = new HashSet<>();
					DatasourceCountry first = it.next()._1();
					countryCodes.add(first.getCountry().getClassid());
					set.add(first.getCountry());
					it.forEachRemaining(t2 -> {
						if (!countryCodes.contains(t2._1().getCountry().getClassid()))
							set.add(t2._1().getCountry());
					});
					rcs.setCountrySet(new ArrayList<>(set));
					return rcs;
				}, Encoders.bean(ResultCountrySet.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}

}
