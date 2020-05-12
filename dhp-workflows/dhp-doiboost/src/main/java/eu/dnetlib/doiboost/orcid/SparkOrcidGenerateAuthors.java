
package eu.dnetlib.doiboost.orcid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class SparkOrcidGenerateAuthors {

	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(SparkOrcidGenerateAuthors.class);
		logger.info("[ SparkOrcidGenerateAuthors STARTED]");
		try {

			final ArgumentApplicationParser parser = new ArgumentApplicationParser(
				IOUtils
					.toString(
						SparkOrcidGenerateAuthors.class
							.getResourceAsStream(
								"/eu/dnetlib/dhp/doiboost/gen_orcid_authors_parameters.json")));
			parser.parseArgument(args);
			Boolean isSparkSessionManaged = Optional
				.ofNullable(parser.get("isSparkSessionManaged"))
				.map(Boolean::valueOf)
				.orElse(Boolean.TRUE);
			logger.info("isSparkSessionManaged: {}", isSparkSessionManaged);
			final String workingDirPath = parser.get("workingPath_orcid");
			logger.info("workingDirPath: ", workingDirPath);

			SparkConf conf = new SparkConf();
			runWithSparkSession(
				conf,
				isSparkSessionManaged,
				spark -> {
					Dataset<Row> lambda = spark.read().load(workingDirPath + "last_modified.csv");
					logger.info("lambda file loaded.");
				});

		} catch (Exception e) {

			logger.info("****************************** " + e.getMessage());
		}

	}

}
