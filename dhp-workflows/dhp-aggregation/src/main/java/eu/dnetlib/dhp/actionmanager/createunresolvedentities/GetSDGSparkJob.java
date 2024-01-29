
package eu.dnetlib.dhp.actionmanager.createunresolvedentities;

import static eu.dnetlib.dhp.actionmanager.Constants.DEFAULT_DELIMITER;
import static eu.dnetlib.dhp.actionmanager.Constants.isSparkSessionManaged;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.actionmanager.createunresolvedentities.model.FOSDataModel;
import eu.dnetlib.dhp.actionmanager.createunresolvedentities.model.SDGDataModel;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class GetSDGSparkJob implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(GetSDGSparkJob.class);

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				GetSDGSparkJob.class
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
			.orElse(DEFAULT_DELIMITER);

		SparkConf sconf = new SparkConf();
		runWithSparkSession(
			sconf,
			isSparkSessionManaged,
			spark -> {
				getSDG(
					spark,
					sourcePath,
					outputPath,
					delimiter);
			});
	}

	private static void getSDG(SparkSession spark, String sourcePath, String outputPath, String delimiter) {
		Dataset<Row> sdgData = spark
			.read()
			.format("csv")
			.option("sep", delimiter)
			.option("inferSchema", "true")
			.option("header", "true")
			.option("quotes", "\"")
			.load(sourcePath);

		sdgData.map((MapFunction<Row, SDGDataModel>) r -> {
			SDGDataModel sdgDataModel = new SDGDataModel();
			sdgDataModel.setDoi(r.getString(0).toLowerCase());
			sdgDataModel.setSbj(r.getString(1));

			return sdgDataModel;
		}, Encoders.bean(SDGDataModel.class))
			.filter((FilterFunction<SDGDataModel>) sdg -> sdg.getSbj() != null)
			.write()
			.mode(SaveMode.Overwrite)
			.json(outputPath);

	}

}
