
package eu.dnetlib.dhp.actionmanager.opencitations;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.opencitations.model.COCI;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import scala.Tuple2;

/**
 * @author miriam.baglioni
 * @Date 29/02/24
 */
public class MapOCIdsInPids implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(CreateActionSetSparkJob.class);
	private static final String DELIMITER = ",";

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(final String[] args) throws IOException, ParseException {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					Objects
						.requireNonNull(
							MapOCIdsInPids.class
								.getResourceAsStream(
									"/eu/dnetlib/dhp/actionmanager/opencitations/remap_parameters.json"))));

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}", outputPath);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> mapIdentifiers(spark, inputPath, outputPath));

	}

	private static void mapIdentifiers(SparkSession spark, String inputPath, String outputPath) {
		Dataset<COCI> coci = spark
			.read()
			.textFile(inputPath + "/JSON")
			.map(
				(MapFunction<String, COCI>) value -> OBJECT_MAPPER.readValue(value, COCI.class),
				Encoders.bean(COCI.class));

		Dataset<Tuple2<String, String>> correspondenceData = spark
			.read()
			.format("csv")
			.option("sep", DELIMITER)
			.option("inferSchema", "true")
			.option("header", "true")
			.option("quotes", "\"")
			.load(inputPath + "/correspondence/omid.zip")
			.repartition(5000)
			.flatMap((FlatMapFunction<Row, Tuple2<String, String>>) r -> {
				String ocIdentifier = r.getAs("omid");
				String[] correspondentIdentifiers = ((String) r.getAs("id")).split(" ");
				return Arrays
					.stream(correspondentIdentifiers)
					.map(ci -> new Tuple2<String, String>(ocIdentifier, ci))
					.collect(Collectors.toList())
					.iterator();
			}, Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

		Dataset<COCI> mappedCitingDataset = coci
			.joinWith(correspondenceData, coci.col("citing").equalTo(correspondenceData.col("_1")), "left")
			.map((MapFunction<Tuple2<COCI, Tuple2<String, String>>, COCI>) t2 -> {
				String correspondent = t2._2()._2();
				t2._1().setCiting_pid(correspondent.substring(0, correspondent.indexOf(":")));
				t2._1().setCiting(correspondent.substring(correspondent.indexOf(":") + 1));
				return t2._1();
			}, Encoders.bean(COCI.class));

		mappedCitingDataset
			.joinWith(correspondenceData, mappedCitingDataset.col("cited").equalTo(correspondenceData.col("_1")))
			.map((MapFunction<Tuple2<COCI, Tuple2<String, String>>, COCI>) t2 -> {
				String correspondent = t2._2()._2();
				t2._1().setCited_pid(correspondent.substring(0, correspondent.indexOf(":")));
				t2._1().setCited(correspondent.substring(correspondent.indexOf(":") + 1));
				return t2._1();
			}, Encoders.bean(COCI.class))
			.write()
			.mode(SaveMode.Append)
			.option("compression", "gzip")
			.json(outputPath);
	}

}
