
package eu.dnetlib.dhp.continuous_validator;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.validator2.validation.XMLApplicationProfile;
import eu.dnetlib.validator2.validation.guideline.openaire.*;
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

			guidelines = parser.get("openaire_guidelines");
			if (guidelines == null) {
				logger
					.error("The \"openaire_guidelines\" was not retrieved from the parameters file: " + parametersFile);
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
		}

		if (!outputPath.endsWith("/"))
			outputPath += "/";

		logger
			.info(
				"Will validate the contents of parquetFile: \"" + parquet_file_path + "\", against guidelines: \""
					+ guidelines + "\"" + " and will output the results in: " + outputPath + RESULTS_FILE_NAME);

		AbstractOpenAireProfile profile;
		switch (guidelines) {
			case "4.0":
				profile = new LiteratureGuidelinesV4Profile();
				break;
			case "3.0":
				profile = new LiteratureGuidelinesV3Profile();
				break;
			case "2.0":
				profile = new DataArchiveGuidelinesV2Profile();
				break;
			case "fair_data":
				profile = new FAIR_Data_GuidelinesProfile();
				break;
			case "fair_literature_v4":
				profile = new FAIR_Literature_GuidelinesV4Profile();
				break;
			default:
				logger.error("Invalid OpenAIRE Guidelines were given: " + guidelines);
				return;
		}

		SparkConf conf = new SparkConf();
		conf.setAppName(ContinuousValidator.class.getSimpleName());
		String finalParquet_file_path = parquet_file_path;
		String finalOutputPath = outputPath;

		runWithSparkSession(conf, isSparkSessionManaged, spark -> {

			// Use a new instance of Document Builder in each worker, as it is not thread-safe.
			MapFunction<Row, XMLApplicationProfile.ValidationResult> validateMapFunction = row -> profile
				.validate(
					row.getAs("id").toString(),
					TestUtils
						.getDocumentBuilder()
						.parse(IOUtils.toInputStream(row.getAs("body").toString(), StandardCharsets.UTF_8)));

			spark
				.read()
				.parquet(finalParquet_file_path)
				.filter("encoding = 'XML' and id is not NULL and body is not NULL")
				.map(validateMapFunction, Encoders.bean(XMLApplicationProfile.ValidationResult.class))
				.write()
				.option("compression", "gzip")
				.mode(SaveMode.Overwrite)
				.json(finalOutputPath + RESULTS_FILE_NAME); // The filename should be the name of the input-file or the

		});
	}
}
