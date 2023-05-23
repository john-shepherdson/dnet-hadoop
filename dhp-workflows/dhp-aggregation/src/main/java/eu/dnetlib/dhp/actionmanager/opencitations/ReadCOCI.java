
package eu.dnetlib.dhp.actionmanager.opencitations;

import static eu.dnetlib.dhp.actionmanager.Constants.DEFAULT_DELIMITER;
import static eu.dnetlib.dhp.actionmanager.Constants.isSparkSessionManaged;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.actionmanager.opencitations.model.COCI;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class ReadCOCI implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(ReadCOCI.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				ReadCOCI.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/opencitations/input_readcoci_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String[] inputFile = parser.get("inputFile").split(";");
		log.info("inputFile {}", inputFile.toString());
		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String workingPath = parser.get("workingPath");
		log.info("workingPath {}", workingPath);

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
					workingPath,
					inputFile,
					outputPath,
					delimiter);
			});
	}

	private static void doRead(SparkSession spark, String workingPath, String[] inputFiles,
		String outputPath,
		String delimiter) throws IOException {

		for (String inputFile : inputFiles) {
			String p_string = workingPath + "/" + inputFile + ".gz";

			Dataset<Row> cociData = spark
				.read()
				.format("csv")
				.option("sep", delimiter)
				.option("inferSchema", "true")
				.option("header", "true")
				.option("quotes", "\"")
				.load(p_string)
				.repartition(100);

			cociData.map((MapFunction<Row, COCI>) row -> {
				COCI coci = new COCI();
				coci.setOci(row.getString(0));
				coci.setCiting(row.getString(1));
				coci.setCited(row.getString(2));
				return coci;
			}, Encoders.bean(COCI.class))
				.write()
				.mode(SaveMode.Overwrite)
				.option("compression", "gzip")
				.json(outputPath + inputFile);
		}

	}

}
