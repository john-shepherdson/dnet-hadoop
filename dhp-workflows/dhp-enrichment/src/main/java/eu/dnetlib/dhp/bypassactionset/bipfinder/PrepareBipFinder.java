
package eu.dnetlib.dhp.bypassactionset.bipfinder;

import static eu.dnetlib.dhp.bypassactionset.Utils.getIdentifier;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.bypassactionset.model.BipDeserialize;
import eu.dnetlib.dhp.bypassactionset.model.BipScore;
import eu.dnetlib.dhp.schema.oaf.Result;

public class PrepareBipFinder implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkUpdateBip.class);
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static <I extends Result> void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				SparkUpdateBip.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/bypassactionset/bip_prepare_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("inputPath");
		log.info("inputPath {}: ", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}: ", outputPath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {

				prepareResults(spark, inputPath, outputPath);
			});
	}

	private static <I extends Result> void prepareResults(SparkSession spark, String inputPath, String outputPath) {

		final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

		JavaRDD<BipDeserialize> bipDeserializeJavaRDD = sc
			.textFile(inputPath)
			.map(item -> OBJECT_MAPPER.readValue(item, BipDeserialize.class));

		spark
			.createDataset(bipDeserializeJavaRDD.flatMap(entry -> entry.keySet().stream().map(key -> {
				BipScore bs = new BipScore();
				bs.setId(getIdentifier(key));
				bs.setScoreList(entry.get(key));
				return bs;
			}).collect(Collectors.toList()).iterator()).rdd(), Encoders.bean(BipScore.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath);
	}
}
