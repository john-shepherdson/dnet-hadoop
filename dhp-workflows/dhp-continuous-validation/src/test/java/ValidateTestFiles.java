import static eu.dnetlib.dhp.continuous_validator.ContinuousValidator.TEST_FILES_V4_DIR;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;

import eu.dnetlib.dhp.continuous_validator.ContinuousValidator;
import eu.dnetlib.validator2.validation.XMLApplicationProfile;
import eu.dnetlib.validator2.validation.guideline.openaire.LiteratureGuidelinesV4Profile;
import eu.dnetlib.validator2.validation.utils.TestUtils;
import scala.Option;

public class ValidateTestFiles {

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ContinuousValidator.class);

	public static final String RESULTS_FILE = "results.json";

	public static void main(String[] args) {
		if (args.length != 3) {
			String errorMsg = "Wrong number of arguments given! PLease run the app like so: java -jar build/libs/continuous-validator-1.0.0-SNAPSHOT.jar <sparkMaster> <parquetFileFullPath> <guidelines>";
			System.err.println(errorMsg);
			logger.error(errorMsg);
			System.exit(1);
		}
		String sparkMaster = args[0];
		logger.info("Will use this Spark master: \"" + sparkMaster + "\".");

		String parquetFileFullPath = args[1];
		String guidelines = args[2];
		logger
			.info(
				"Will validate the contents of parquetFile: \"" + parquetFileFullPath + "\", against guidelines: \""
					+ guidelines + "\".");

		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Continuous-Validator");
		sparkConf.setMaster(sparkMaster); // Run on the Spark Cluster.
		sparkConf.set("spark.driver.memory", "4096M");
		sparkConf
			.set("spark.executor.instances", "4") // 4 executors
			.set("spark.executor.cores", "1"); // 1 core per executor
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sparkConf.set("spark.rdd.compress", "true");

		String appVersion = "1.0.0-SNAPSHOT";
		/*
		 * try { Class<?> klass = Class.forName("eu.dnetlib.continuous_validator.BuildConfig"); appVersion = (String)
		 * klass.getDeclaredField("version").get(null); if ( logger.isTraceEnabled() )
		 * logger.trace("The app's version is: " + appVersion); } catch (Exception e) {
		 * logger.error("Error when acquiring the \"appVersion\"!", e); System.exit(1); }
		 */

		sparkConf.setJars(new String[] {
			"build/libs/continuous-validator-" + appVersion + "-all.jar"
		}); // This is the "fat-Jar".
		sparkConf.validateSettings();

		logger.debug("Spark custom configurations: " + sparkConf.getAll().toString());

		LiteratureGuidelinesV4Profile profile = new LiteratureGuidelinesV4Profile();

		try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
			JavaPairRDD<String, String> jprdd = sc.wholeTextFiles(TEST_FILES_V4_DIR);

			logger.info("Showing the validation-results..");

			// Use a new instance of Document Builder in each worker, as it is not thread-safe.
			// The "x._1" is the filename and the "x._2" in the content of the file.
			List<XMLApplicationProfile.ValidationResult> validationResultsList = jprdd
				.map(
					x -> profile
						.validate(
							x._1,
							TestUtils.getDocumentBuilder().parse(IOUtils.toInputStream(x._2, StandardCharsets.UTF_8))))
				.collect();

			if (validationResultsList.isEmpty()) {
				logger.error("The \"validationResultsList\" was empty!");
				return;
			}

			if (logger.isDebugEnabled())
				validationResultsList.forEach(vr -> logger.debug(vr.id() + " | score:" + vr.score()));

			logger.debug(validationResultsList.toString());

			try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(RESULTS_FILE), StandardCharsets.UTF_8)) {
				writer.write(new Gson().toJson(validationResultsList));
			} catch (Exception e) {
				logger
					.error(
						"Error when writing the \"validationResultsList\" as json into the results-file: "
							+ RESULTS_FILE);
				return;
			}

			Option<String> uiWebUrl = sc.sc().uiWebUrl();
			if (uiWebUrl.isDefined()) {
				logger
					.info(
						"Waiting 60 seconds, before shutdown, for the user to check the jobs' status at: "
							+ uiWebUrl.get());
				Thread.sleep(60_000);
			} else
				logger.info("The \"uiWebUrl\" is not defined, in order to check the jobs' status. Shutting down..");

		} catch (JsonIOException jie) {
			logger.error("Error when writing the validation results to the json file: " + jie.getMessage());
		} catch (Exception e) {
			logger.error("Error validating directory: " + TEST_FILES_V4_DIR, e);
		}
	}

}
