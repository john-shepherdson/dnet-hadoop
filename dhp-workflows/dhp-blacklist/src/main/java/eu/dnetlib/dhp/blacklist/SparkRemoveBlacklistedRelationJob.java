
package eu.dnetlib.dhp.blacklist;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class SparkRemoveBlacklistedRelationJob {
	private static final Logger log = LoggerFactory.getLogger(SparkRemoveBlacklistedRelationJob.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkRemoveBlacklistedRelationJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/blacklist/sparkblacklist_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		final String blacklistPath = parser.get("hdfsPath");
		log.info("blacklistPath {}: ", blacklistPath);

		final String mergesPath = parser.get("mergesPath");
		log.info("mergesPath {}: ", mergesPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				removeBlacklistedRelations(
					spark,
					inputPath,
					blacklistPath,
					outputPath,
					mergesPath);
			});

	}

	private static void removeBlacklistedRelations(SparkSession spark, String blacklistPath, String inputPath,
		String outputPath, String mergesPath) {
		Dataset<Relation> blackListed = readRelations(spark, blacklistPath);
		Dataset<Relation> inputRelation = readRelations(spark, inputPath);
		Dataset<Relation> mergesRelation = readRelations(spark, mergesPath);

		Dataset<Relation> dedupSource = blackListed
			.joinWith(mergesRelation, blackListed.col("source").equalTo(mergesRelation.col("target")), "left_outer")
			.map(c -> {
				Optional<Relation> merged = Optional.ofNullable(c._2());
				Relation bl = c._1();
				if (merged.isPresent()) {
					bl.setSource(merged.get().getSource());
				}
				return bl;
			}, Encoders.bean(Relation.class));

		Dataset<Relation> dedupBL = dedupSource
			.joinWith(mergesRelation, dedupSource.col("target").equalTo(mergesRelation.col("target")), "left_outer")
			.map(c -> {
				Optional<Relation> merged = Optional.ofNullable(c._2());
				Relation bl = c._1();
				if (merged.isPresent()) {
					bl.setTarget(merged.get().getSource());
				}
				return bl;
			}, Encoders.bean(Relation.class));

		inputRelation
			.joinWith(dedupBL, inputRelation.col("source").equalTo(dedupBL.col("source")), "left_outer")
			.map(c -> {
				Relation ir = c._1();
				Optional<Relation> obl = Optional.ofNullable(c._2());
				if (obl.isPresent()) {
					if (ir.equals(obl.get())) {
						return null;
					}
				}
				return ir;

			}, Encoders.bean(Relation.class))
			.filter(r -> !(r == null))
			.toJSON()
			.write()
			.mode(SaveMode.Overwrite)
			.option("conpression", "gzip")
			.text(outputPath);

	}

	public static org.apache.spark.sql.Dataset<Relation> readRelations(
		SparkSession spark, String inputPath) {
		return spark
			.read()
			.textFile(inputPath)
			.map(
				(MapFunction<String, Relation>) value -> OBJECT_MAPPER.readValue(value, Relation.class),
				Encoders.bean(Relation.class));
	}

}
