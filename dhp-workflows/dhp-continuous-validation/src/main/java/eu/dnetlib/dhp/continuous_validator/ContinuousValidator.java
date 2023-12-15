
package eu.dnetlib.dhp.continuous_validator;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.jws.WebParam;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.validator2.validation.XMLApplicationProfile;
import eu.dnetlib.validator2.validation.guideline.openaire.AbstractOpenAireProfile;
import eu.dnetlib.validator2.validation.guideline.openaire.LiteratureGuidelinesV4Profile;
import eu.dnetlib.validator2.validation.utils.TestUtils;
import scala.Option;

public class ContinuousValidator {

	public static final String TEST_FILES_V4_DIR = TestUtils.TEST_FILES_BASE_DIR + "openaireguidelinesV4/";
	public static final String RESULTS_FILE_NAME = "results.json";
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ContinuousValidator.class);
	private static final String parametersFile = "input_continuous_validator_parameters.json";

	private static final boolean SHOULD_GET_ARGUMENTS_FROM_FILE = true; // It throws an error for now..

	public static void main(String[] args) {

		ArgumentApplicationParser parser = null;
		String sparkMaster = null;
		Boolean isSparkSessionManaged = false;
		String parquet_file_path = null;
		String guidelines = null;
		String outputPath = null;

		if (SHOULD_GET_ARGUMENTS_FROM_FILE) {
			try {
				String jsonConfiguration = IOUtils
					.toString(
						Objects
							.requireNonNull(
								ContinuousValidator.class
									.getResourceAsStream("/eu/dnetlib/dhp/continuous_validator/" + parametersFile)),
						StandardCharsets.UTF_8);

				parser = new ArgumentApplicationParser(jsonConfiguration);
				parser.parseArgument(args);
			} catch (Exception e) {
				logger.error("Error when parsing the parameters!", e);
				return;
			}

			isSparkSessionManaged = Optional
				.ofNullable(parser.get("isSparkSessionManaged")) // This param is not mandatory, so it may be null.
				.map(Boolean::valueOf)
				.orElse(Boolean.TRUE);

			logger.info("isSparkSessionManaged: {}", isSparkSessionManaged);
			// This is needed to implement a unit test in which the spark session is created in the context of the
			// unit test itself rather than inside the spark application"

			parquet_file_path = parser.get("parquet_file_path");
			if (parquet_file_path == null) {
				logger.error("The \"parquet_file_path\" was not retrieved from the parameters file: " + parametersFile);
				return;
			}

			guidelines = parser.get("guidelines");
			if (guidelines == null) {
				logger.error("The \"guidelines\" was not retrieved from the parameters file: " + parametersFile);
				return;
			}

			outputPath = parser.get("outputPath");
			if (outputPath == null) {
				logger.error("The \"outputPath\" was not retrieved from the parameters file: " + parametersFile);
				return;
			}

			sparkMaster = "local[*]";
		} else {
			if (args.length != 4) {
				String errorMsg = "Wrong number of arguments given! Please run the app like so: java -jar target/dhp-continuous-validation-1.0.0-SNAPSHOT.jar <sparkMaster> <parquetFileFullPath> <guidelines> <outputPath>";
				System.err.println(errorMsg);
				logger.error(errorMsg);
				System.exit(1);
			}
			sparkMaster = args[0];
			logger.info("Will use this Spark master: \"" + sparkMaster + "\".");

			parquet_file_path = args[1];
			guidelines = args[2];
			outputPath = args[3];
			if (!outputPath.endsWith("/"))
				outputPath += "/";
		}

		logger
			.info(
				"Will validate the contents of parquetFile: \"" + parquet_file_path + "\", against guidelines: \""
					+ guidelines + "\"" + " and will output the results in: " + outputPath + RESULTS_FILE_NAME);

		AbstractOpenAireProfile profile = new LiteratureGuidelinesV4Profile();

		SparkConf conf = new SparkConf();
		conf.setAppName(ContinuousValidator.class.getSimpleName());
		String finalParquet_file_path = parquet_file_path;
		String finalOutputPath = outputPath;

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {
			Dataset<Row> parquetFileDF = spark.read().parquet(finalParquet_file_path);
			parquetFileDF.show(5);

			// Filter the results based on the XML-encoding and non-null id and body.
			parquetFileDF = parquetFileDF
				.filter(
					parquetFileDF
						.col("encoding")
						.eqNullSafe("XML")
						.and(parquetFileDF.col("id").isNotNull())
						.and(parquetFileDF.col("body").isNotNull()));

			// Use a new instance of Document Builder in each worker, as it is not thread-safe.
			MapFunction<Row, XMLApplicationProfile.ValidationResult> validateMapFunction = row -> profile
				.validate(
					row.getAs("id").toString(),
					TestUtils
						.getDocumentBuilder()
						.parse(IOUtils.toInputStream(row.getAs("body").toString(), StandardCharsets.UTF_8)));

			Dataset<XMLApplicationProfile.ValidationResult> validationResultsDataset = parquetFileDF
				.map(validateMapFunction, Encoders.bean(XMLApplicationProfile.ValidationResult.class));

			logger.info("Showing a few validation-results.. just for checking");
			validationResultsDataset.show(5);

			// Write the results to json file immediately, without converting them to a list.
			validationResultsDataset
				.write()
				.option("compression", "gzip")
				.mode(SaveMode.Overwrite)
				.json(finalOutputPath + RESULTS_FILE_NAME); // The filename should be the name of the input-file or the
														// input-directory.

			if (logger.isDebugEnabled()) {
				List<XMLApplicationProfile.ValidationResult> validationResultsList = validationResultsDataset
					.javaRDD()
					.collect();

				if (validationResultsList.isEmpty()) {
					logger.error("The \"validationResultsList\" was empty!");
					return;
				}

				validationResultsList.forEach(vr -> logger.debug(vr.id() + " | score:" + vr.score()));
				for (XMLApplicationProfile.ValidationResult result : validationResultsList)
					logger.debug(result.toString());
			}

			// TODO - REMOVE THIS WHEN THE WRITE FROM ABOVE IS OK
			/*
			 * try (BufferedWriter writer = Files .newBufferedWriter(Paths.get(outputPath + RESULTS_FILE),
			 * StandardCharsets.UTF_8)) { writer.write(new Gson().toJson(validationResultsList)); } catch (Exception e)
			 * { logger.error("Error when writing the \"validationResultsList\" as json into the results-file: " +
			 * outputPath + RESULTS_FILE); return; }
			 */

			Option<String> uiWebUrl = spark.sparkContext().uiWebUrl();
			if (uiWebUrl.isDefined()) {
				logger
					.info(
						"Waiting 60 seconds, before shutdown, for the user to check the jobs' status at: "
							+ uiWebUrl.get());
				try {
					Thread.sleep(60_000);
				} catch (InterruptedException ignored) {
				}
			} else
				logger.info("The \"uiWebUrl\" is not defined, in order to check the jobs' status. Shutting down..");
		});
	}
}
