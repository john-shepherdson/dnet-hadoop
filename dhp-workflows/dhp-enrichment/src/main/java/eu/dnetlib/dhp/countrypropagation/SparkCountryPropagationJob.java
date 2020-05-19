
package eu.dnetlib.dhp.countrypropagation;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Country;
import eu.dnetlib.dhp.schema.oaf.Result;
import scala.Tuple2;

public class SparkCountryPropagationJob {

	private static final Logger log = LoggerFactory.getLogger(SparkCountryPropagationJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkCountryPropagationJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/countrypropagation/input_countrypropagation_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String sourcePath = parser.get("sourcePath");
		log.info("sourcePath: {}", sourcePath);

		String preparedInfoPath = parser.get("preparedInfoPath");
		log.info("preparedInfoPath: {}", preparedInfoPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		final Boolean saveGraph = Optional
			.ofNullable(parser.get("saveGraph"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("saveGraph: {}", saveGraph);

		Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeOutputDir(spark, outputPath);
				execPropagation(
					spark,
					sourcePath,
					preparedInfoPath,
					outputPath,
					resultClazz,
					saveGraph);
			});
	}

	private static <R extends Result> void execPropagation(
		SparkSession spark,
		String sourcePath,
		String preparedInfoPath,
		String outputPath,
		Class<R> resultClazz,
		boolean saveGraph) {

		if (saveGraph) {
			// updateResultTable(spark, potentialUpdates, inputPath, resultClazz, outputPath);
			log.info("Reading Graph table from: {}", sourcePath);
			Dataset<R> res = readPath(spark, sourcePath, resultClazz);

			log.info("Reading prepared info: {}", preparedInfoPath);
			Dataset<ResultCountrySet> prepared = spark
				.read()
				.json(preparedInfoPath)
				.as(Encoders.bean(ResultCountrySet.class));

			res
				.joinWith(prepared, res.col("id").equalTo(prepared.col("resultId")), "left_outer")
				.map(getCountryMergeFn(), Encoders.bean(resultClazz))
				.write()
				.option("compression", "gzip")
				.mode(SaveMode.Overwrite)
				.json(outputPath);
		}
	}

	private static <R extends Result> MapFunction<Tuple2<R, ResultCountrySet>, R> getCountryMergeFn() {
		return (MapFunction<Tuple2<R, ResultCountrySet>, R>) t -> {
			Optional.ofNullable(t._2()).ifPresent(r -> {
				t._1().getCountry().addAll(merge(t._1().getCountry(), r.getCountrySet()));
			});
			return t._1();
		};
	}

	private static List<Country> merge(List<Country> c1, List<CountrySbs> c2) {
		HashSet<String> countries = c1
			.stream()
			.map(c -> c.getClassid())
			.collect(Collectors.toCollection(HashSet::new));

		return c2
			.stream()
			.filter(c -> !countries.contains(c.getClassid()))
			.map(c -> getCountry(c.getClassid(), c.getClassname()))
			.collect(Collectors.toList());
	}

}
