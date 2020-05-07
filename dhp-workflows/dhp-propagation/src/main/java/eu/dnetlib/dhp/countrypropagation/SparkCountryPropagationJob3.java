
package eu.dnetlib.dhp.countrypropagation;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelSupport;
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

			spark
				.read()
				.json(inputPath)
				.as(Encoders.bean(resultClazz))
				.groupByKey((MapFunction<R, String>) result1 -> result1.getId(), Encoders.STRING())
				.mapGroups(getCountryMergeFn(resultClazz), Encoders.bean(resultClazz))
				.write()
				.option("compression", "gzip")
				.mode(SaveMode.Overwrite)
				.json(outputPath);
		}
	}

	private static <R extends Result> MapGroupsFunction<String, R, R> getCountryMergeFn(Class<R> resultClazz) {
		return (MapGroupsFunction<String, R, R>) (key, values) -> {
			R res = resultClazz.newInstance();
			List<Country> countries = new ArrayList<>();
			values.forEachRemaining(r -> {
				res.mergeFrom(r);
				countries.addAll(r.getCountry());
			});
			res
				.setCountry(
					countries
						.stream()
						.collect(
							Collectors
								.toMap(
									Country::getClassid,
									Function.identity(),
									(c1, c2) -> {
										if (Optional
											.ofNullable(
												c1.getDataInfo().getInferenceprovenance())
											.isPresent()) {
											return c2;
										}
										return c1;
									}))
						.values()
						.stream()
						.collect(Collectors.toList()));
			return res;
		};
	}

}
