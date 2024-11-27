
package eu.dnetlib.dhp.countrypropagation;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Country;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Result;
import scala.Tuple2;

public class SparkCountryPropagationJob {

	private static final Logger log = LoggerFactory.getLogger(SparkCountryPropagationJob.class);

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkCountryPropagationJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/wf/subworkflows/countrypropagation/input_countrypropagation_parameters.json"));

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
					resultClazz);
			});
	}

	private static <R extends Result> void execPropagation(
		SparkSession spark,
		String sourcePath,
		String preparedInfoPath,
		String outputPath,
		Class<R> resultClazz) {

		log.info("Reading Graph table from: {}", sourcePath);
		Dataset<R> res = readPath(spark, sourcePath, resultClazz);

		log.info("Reading prepared info: {}", preparedInfoPath);
		Encoder<ResultCountrySet> rcsEncoder = Encoders.bean(ResultCountrySet.class);
		final Dataset<ResultCountrySet> preparedInfoRaw = spark
			.read()
			.schema(rcsEncoder.schema())
			.json(preparedInfoPath)
			.as(rcsEncoder);

		if (!preparedInfoRaw.isEmpty()) {
			res
				.joinWith(preparedInfoRaw, res.col("id").equalTo(preparedInfoRaw.col("resultId")), "left_outer")
				.map(getCountryMergeFn(), Encoders.bean(resultClazz))
				.write()
				.option("compression", "gzip")
				.mode(SaveMode.Overwrite)
				.json(outputPath);
		} else {
			res
				.write()
				.option("compression", "gzip")
				.mode(SaveMode.Overwrite)
				.json(outputPath);
		}
	}

	private static <R extends Result> MapFunction<Tuple2<R, ResultCountrySet>, R> getCountryMergeFn() {
		return t -> {
			Optional.ofNullable(t._2()).ifPresent(r -> {
				if (Optional.ofNullable(t._1().getCountry()).isPresent())
					t._1().getCountry().addAll(merge(t._1().getCountry(), r.getCountrySet()));
				else
					t._1().setCountry(merge(null, t._2().getCountrySet()));
			});
			return t._1();
		};
	}

	private static List<Country> merge(List<Country> c1, List<CountrySbs> c2) {
		HashSet<String> countries = new HashSet<>();
		if (Optional.ofNullable(c1).isPresent()) {
			countries = c1
				.stream()
				.map(Qualifier::getClassid)
				.collect(Collectors.toCollection(HashSet::new));
		}

		HashSet<String> finalCountries = countries;
		return c2
			.stream()
			.filter(c -> !finalCountries.contains(c.getClassid()))
			.map(c -> getCountry(c.getClassid(), c.getClassname()))
			.collect(Collectors.toList());
	}

}
