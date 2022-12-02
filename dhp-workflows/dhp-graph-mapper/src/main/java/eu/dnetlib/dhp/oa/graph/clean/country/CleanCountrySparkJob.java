
package eu.dnetlib.dhp.oa.graph.clean.country;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.swing.text.html.Option;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author miriam.baglioni
 * @Date 20/07/22
 */
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.clean.CleanContextSparkJob;
import eu.dnetlib.dhp.schema.oaf.Country;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.schema.oaf.utils.PidType;

public class CleanCountrySparkJob implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(CleanCountrySparkJob.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				CleanCountrySparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/input_clean_country_parameters.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("inputPath");
		log.info("inputPath: {}", inputPath);

		String workingDir = parser.get("workingDir");
		log.info("workingDir: {}", workingDir);

		String datasourcePath = parser.get("hostedBy");
		log.info("datasourcePath: {}", datasourcePath);

		String country = parser.get("country");
		log.info("country: {}", country);

		String[] verifyParam = parser.get("verifyParam").split(";");
		log.info("verifyParam: {}", verifyParam);

		String collectedfrom = parser.get("collectedfrom");
		log.info("collectedfrom: {}", collectedfrom);

		String graphTableClassName = parser.get("graphTableClassName");
		log.info("graphTableClassName: {}", graphTableClassName);

		Class<? extends Result> entityClazz = (Class<? extends Result>) Class.forName(graphTableClassName);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {

				cleanCountry(
					spark, country, verifyParam, inputPath, entityClazz, workingDir, collectedfrom, datasourcePath);
			});
	}

	private static <T extends Result> void cleanCountry(SparkSession spark, String country, String[] verifyParam,
		String inputPath, Class<T> entityClazz, String workingDir, String collectedfrom, String datasourcePath) {

		List<String> hostedBy = spark
			.read()
			.textFile(datasourcePath)
			.collectAsList();

		Dataset<T> res = spark
			.read()
			.textFile(inputPath)
			.map(
				(MapFunction<String, T>) value -> OBJECT_MAPPER.readValue(value, entityClazz),
				Encoders.bean(entityClazz));

		res.map((MapFunction<T, T>) r -> {
			if (r.getInstance().stream().anyMatch(i -> hostedBy.contains(i.getHostedby().getKey())) ||
				!r.getCollectedfrom().stream().anyMatch(cf -> cf.getValue().equals(collectedfrom))) {
				return r;
			}

			if (r
				.getPid()
				.stream()
				.anyMatch(
					p -> p
						.getQualifier()
						.getClassid()
						.equals(PidType.doi.toString()) && pidInParam(p.getValue(), verifyParam))) {
				r
					.setCountry(
						r
							.getCountry()
							.stream()
							.filter(
								c -> toTakeCountry(c, country))
							.collect(Collectors.toList()));

			}

			return r;
		}, Encoders.bean(entityClazz))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(workingDir);

		spark
			.read()
			.textFile(workingDir)
			.map(
				(MapFunction<String, T>) value -> OBJECT_MAPPER.readValue(value, entityClazz),
				Encoders.bean(entityClazz))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(inputPath);
	}

	private static boolean pidInParam(String value, String[] verifyParam) {
		for (String s : verifyParam)
			if (value.startsWith(s))
				return true;
		return false;
	}

	private static boolean toTakeCountry(Country c, String country) {
		// If dataInfo is not set, or dataInfo.inferenceprovenance is not set or not present then it cannot be
		// inserted via propagation
		if (!Optional.ofNullable(c.getDataInfo()).isPresent())
			return true;
		if (!Optional.ofNullable(c.getDataInfo().getInferenceprovenance()).isPresent())
			return true;
		return !(c
			.getClassid()
			.equalsIgnoreCase(country) &&
			c.getDataInfo().getInferenceprovenance().equals("propagation"));
	}

}
