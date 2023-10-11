
package eu.dnetlib.dhp.actionmanager.createunresolvedentities;

import static eu.dnetlib.dhp.actionmanager.Constants.DEFAULT_FOS_DELIMITER;
import static eu.dnetlib.dhp.actionmanager.Constants.isSparkSessionManaged;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.actionmanager.createunresolvedentities.model.FOSDataModel;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class GetFOSSparkJob implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(GetFOSSparkJob.class);

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				GetFOSSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/createunresolvedentities/get_input_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		// the path where the original fos csv file is stored
		final String sourcePath = parser.get("sourcePath");
		log.info("sourcePath {}", sourcePath);

		// the path where to put the file as json
		final String outputPath = parser.get("outputPath");
		log.info("outputPath {}", outputPath);

		final String delimiter = Optional
			.ofNullable(parser.get("delimiter"))
			.orElse(DEFAULT_FOS_DELIMITER);

		SparkConf sconf = new SparkConf();
		runWithSparkSession(
			sconf,
			isSparkSessionManaged,
			spark -> {
				getFOS(
					spark,
					sourcePath,
					outputPath,
					delimiter);
			});
	}

	private static void getFOS(SparkSession spark, String sourcePath, String outputPath, String delimiter) {
		Dataset<Row> fosData = spark
			.read()
			.format("csv")
			.option("sep", delimiter)
			.option("inferSchema", "true")
			.option("header", "true")
			.option("quotes", "\"")
			.load(sourcePath);

		fosData.map((MapFunction<Row, FOSDataModel>) r -> {
			FOSDataModel fosDataModel = new FOSDataModel();
			fosDataModel.setDoi(r.getString(0).toLowerCase());
			fosDataModel.setLevel1(r.getString(2));
			fosDataModel.setLevel2(r.getString(3));
			fosDataModel.setLevel3(r.getString(4));
			fosDataModel.setLevel4(r.getString(5));
			fosDataModel.setScoreL3(String.valueOf(r.getDouble(6)));
			fosDataModel.setScoreL4(String.valueOf(r.getDouble(7)));
			return fosDataModel;
		}, Encoders.bean(FOSDataModel.class))
			.write()
			.mode(SaveMode.Overwrite)
			.json(outputPath);

	}

}
