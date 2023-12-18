
package eu.dnetlib.dhp.actionmanager.transformativeagreement;

import static eu.dnetlib.dhp.actionmanager.Constants.DEFAULT_DELIMITER;
import static eu.dnetlib.dhp.actionmanager.Constants.isSparkSessionManaged;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.actionmanager.opencitations.model.COCI;
import eu.dnetlib.dhp.actionmanager.transformativeagreement.model.TransformativeAgreementModel;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class ReadTransformativeAgreement implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(ReadTransformativeAgreement.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				ReadTransformativeAgreement.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/transformativeagreement/input_read_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String inputFile = parser.get("inputFile");
		log.info("inputFile {}", inputFile);
		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		SparkConf sconf = new SparkConf();

		final String delimiter = Optional
			.ofNullable(parser.get("delimiter"))
			.orElse(DEFAULT_DELIMITER);

		runWithSparkSession(
			sconf,
			isSparkSessionManaged,
			spark -> {
				doRead(
					spark,
					inputFile,
					outputPath,
					delimiter);
			});
	}

	private static void doRead(SparkSession spark, String inputFile,
		String outputPath,
		String delimiter) {

		Dataset<Row> data = spark
			.read()
			.format("csv")
			.option("sep", delimiter)
			.option("inferSchema", "true")
			.option("header", "true")
			.load(inputFile)
			.repartition(100);

		data.map((MapFunction<Row, TransformativeAgreementModel>) row -> {
			TransformativeAgreementModel trm = new TransformativeAgreementModel();

			trm.setInstitution(row.getString(2));
			trm.setDoi(row.getString(7));

			return trm;
		}, Encoders.bean(TransformativeAgreementModel.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + inputFile);
	}

}
