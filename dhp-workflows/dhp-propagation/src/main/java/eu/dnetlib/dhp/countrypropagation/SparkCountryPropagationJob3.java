
package eu.dnetlib.dhp.countrypropagation;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Result;
import scala.Tuple2;

public class SparkCountryPropagationJob3 {

	private static final Logger log = LoggerFactory.getLogger(SparkCountryPropagationJob3.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkCountryPropagationJob3.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/countrypropagation/input_countrypropagation_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

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

				execPropagation(
					spark,
					inputPath,
					outputPath,
					resultClazz,
					saveGraph);
			});
	}

	private static <R extends Result> void execPropagation(
		SparkSession spark,
		String inputPath,
		String outputPath,
		Class<R> resultClazz,
		boolean saveGraph) {

		if (saveGraph) {
			// updateResultTable(spark, potentialUpdates, inputPath, resultClazz, outputPath);
			log.info("Reading Graph table from: {}", inputPath);
			final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
			JavaPairRDD<String, R> results = sc
				.textFile(inputPath)
				.map(r -> OBJECT_MAPPER.readValue(r, resultClazz))
				.mapToPair(r -> new Tuple2<>(r.getId(), r));

			JavaPairRDD<String, R> tmp = results.reduceByKey((r1, r2) -> {
				if (r1 == null) {
					return r2;
				}
				if (r2 == null) {
					return r1;
				}
				if (Optional.ofNullable(r1.getCollectedfrom()).isPresent()) {
					r1.setCountry(getUnionCountries(r1.getCountry(), r2.getCountry()));
					return r1;
				}
				if (Optional.ofNullable(r2.getCollectedfrom()).isPresent()) {
					r2.setCountry(getUnionCountries(r1.getCountry(), r2.getCountry()));
					return r2;
				}
				r1.setCountry(getUnionCountries(r1.getCountry(), r2.getCountry()));
				return r1;
			});

			tmp
				.map(c -> c._2())
				.map(r -> OBJECT_MAPPER.writeValueAsString(r))
				.saveAsTextFile(outputPath, GzipCodec.class);
		}
	}

	private static List<Country> getUnionCountries(List<Country> country, List<Country> country1) {
		HashSet<String> countries = country
			.stream()
			.map(c -> c.getClassid())
			.collect(Collectors.toCollection(HashSet::new));
		country
			.addAll(
				country1
					.stream()
					.filter(c -> !(countries.contains(c.getClassid())))
					.collect(Collectors.toList()));
		return country;
	}

}
