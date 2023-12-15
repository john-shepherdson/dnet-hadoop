import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;

import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.continuous_validator.ContinuousValidator;

public class ReadResultsTest {

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ContinuousValidator.class);

	public static void main(String[] args) {

		try {
			List standardValidationResultList = new Gson()
				.fromJson(new BufferedReader(new FileReader(ContinuousValidator.RESULTS_FILE)), List.class);
			if (standardValidationResultList == null)
				logger.error("Could not map the json to a \"List\" object.");
			else if (standardValidationResultList.isEmpty())
				logger.warn("The \"standardValidationResultList\" is empty!");
			else
				logger.info(standardValidationResultList.toString());
		} catch (FileNotFoundException fnfe) {
			logger.error("The results-file \"" + ContinuousValidator.RESULTS_FILE + "\" does not exist!");
		} catch (Exception e) {
			logger.error("Error when reading the json-results-file \"" + ContinuousValidator.RESULTS_FILE + "\"", e);
		}
	}

}
