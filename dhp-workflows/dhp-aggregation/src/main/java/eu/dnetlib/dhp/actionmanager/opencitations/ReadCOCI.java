
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

		final String hdfsNameNode = parser.get("nameNode");
		log.info("nameNode: {}", hdfsNameNode);

		final String inputPath = parser.get("sourcePath");
		log.info("input path : {}", inputPath);
		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsNameNode);

		FileSystem fileSystem = FileSystem.get(conf);
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
					fileSystem,
					inputPath,
					outputPath,
					delimiter);
			});
	}

	public static void doRead(SparkSession spark, FileSystem fileSystem, String inputPath, String outputPath,
		String delimiter) throws IOException {

		RemoteIterator<LocatedFileStatus> iterator = fileSystem
			.listFiles(
				new Path(inputPath), true);

		while (iterator.hasNext()) {
			LocatedFileStatus fileStatus = iterator.next();

			Path p = fileStatus.getPath();
			String p_string = p.toString();
			Dataset<Row> cociData = spark
				.read()
				.format("csv")
				.option("sep", delimiter)
				.option("inferSchema", "true")
				.option("header", "true")
				.option("quotes", "\"")
				.load(p_string);

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
				.json(outputPath + "/" + p_string.substring(p_string.lastIndexOf("/") + 1));
		}

	}

}
