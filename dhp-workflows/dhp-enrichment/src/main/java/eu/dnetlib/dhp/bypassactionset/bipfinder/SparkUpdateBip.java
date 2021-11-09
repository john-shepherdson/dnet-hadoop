
package eu.dnetlib.dhp.bypassactionset.bipfinder;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.bypassactionset.model.BipScore;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

/**
 * created the Atomic Action for each tipe of results
 */
public class SparkUpdateBip implements Serializable {


	private static final Logger log = LoggerFactory.getLogger(SparkUpdateBip.class);

	public static <I extends Result> void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
					SparkUpdateBip.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/bipfinder/input_parameters.json"));

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

		final String bipScorePath = parser.get("bipScorePath");
		log.info("bipScorePath: {}", bipScorePath);

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		Class<I> inputClazz = (Class<I>) Class.forName(resultClassName);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark ->
				updateBipFinder(spark, inputPath, outputPath, bipScorePath, inputClazz)

			);
	}

	private static <I extends Result> void updateBipFinder(SparkSession spark, String inputPath, String outputPath,
		String bipScorePath, Class<I> inputClazz) {

		Dataset<I> results = readPath(spark, inputPath, inputClazz);
		Dataset<BipScore> bipScores = readPath(spark, bipScorePath, BipScore.class);

		results.joinWith(bipScores, results.col("id").equalTo(bipScores.col("id")), "left")
				.map((MapFunction<Tuple2<I,BipScore>, I>) value -> {
					if (!Optional.ofNullable(value._2()).isPresent()){
						return value._1();
					}
					value._1().setMeasures(getMeasure(value._2()));
					return value._1();
				}, Encoders.bean(inputClazz))
		.write()
		.mode(SaveMode.Overwrite)
		.option("compression","gzip")
		.json(outputPath + "/bip");

	}

	private static List<Measure> getMeasure(BipScore value) {
		return value
			.getScoreList()
			.stream()
			.map(score -> {
				Measure m = new Measure();
				m.setId(score.getId());
				m
					.setUnit(
						score
							.getUnit()
							.stream()
							.map(unit -> {
								KeyValue kv = new KeyValue();
								kv.setValue(unit.getValue());
								kv.setKey(unit.getKey());
								kv.setDataInfo(getDataInfo(UPDATE_DATA_INFO_TYPE,
										UPDATE_MEASURE_BIP_CLASS_ID,
										UPDATE_MEASURE_BIP_CLASS_NAME,
										ModelConstants.DNET_PROVENANCE_ACTIONS, ""));
								return kv;
							})
							.collect(Collectors.toList()));
				return m;
			})
			.collect(Collectors.toList());
	}







}
