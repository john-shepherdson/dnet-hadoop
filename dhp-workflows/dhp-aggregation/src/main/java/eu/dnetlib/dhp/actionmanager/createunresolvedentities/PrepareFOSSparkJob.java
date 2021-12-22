
package eu.dnetlib.dhp.actionmanager.createunresolvedentities;

import static eu.dnetlib.dhp.actionmanager.Constants.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.actionmanager.createunresolvedentities.model.FOSDataModel;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.utils.DHPUtils;

public class PrepareFOSSparkJob implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(PrepareFOSSparkJob.class);

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				PrepareFOSSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/actionmanager/createunresolvedentities/prepare_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);

		parser.parseArgument(args);

		Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		String sourcePath = parser.get("sourcePath");
		log.info("sourcePath: {}", sourcePath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				distributeFOSdois(
					spark,
					sourcePath,

					outputPath);
			});
	}

	private static void distributeFOSdois(SparkSession spark, String sourcePath, String outputPath) {
		Dataset<FOSDataModel> fosDataset = readPath(spark, sourcePath, FOSDataModel.class);

		fosDataset.map((MapFunction<FOSDataModel, Result>) value -> {
				Result r = new Result();
				r.setId(DHPUtils.generateUnresolvedIdentifier(value.getDoi(), DOI));
				r.setSubject(getSubjects(value));
				return r;
			}, Encoders.bean(Result.class))
			.write()
			.mode(SaveMode.Overwrite)
			.option("compression", "gzip")
			.json(outputPath + "/fos");
	}

	private static List<StructuredProperty> getSubjects(FOSDataModel fos) {
		return Arrays
			.asList(getSubject(fos.getLevel1(), FOS_CLASS_ID, FOS_CLASS_NAME),
					getSubject(fos.getLevel2(), FOS_CLASS_ID, FOS_CLASS_NAME),
					getSubject(fos.getLevel3(), FOS_CLASS_ID, FOS_CLASS_NAME))
			.stream()
			.filter(Objects::nonNull)
			.collect(Collectors.toList());
	}



}
