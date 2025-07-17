
package eu.dnetlib.dhp.oa.merge;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpException;

/**
 * Copy specified entities from a graph snapshot to another
 */
public class CleanFolderSparkJob {
	private static final Logger log = LoggerFactory.getLogger(CleanFolderSparkJob.class);

	private ArgumentApplicationParser parser;

	public CleanFolderSparkJob(ArgumentApplicationParser parser) {
		this.parser = parser;
	}

	public static void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				CleanFolderSparkJob.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/merge/clean_folder_parameters.json"));
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		new CleanFolderSparkJob(parser).run(isSparkSessionManaged);
	}

	public void run(Boolean isSparkSessionManaged)
		throws ISLookUpException {

		String deletePath = parser.get("deletePath");
		log.info("deletePath: {}", deletePath);

		SparkConf conf = new SparkConf();

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				HdfsSupport.remove(deletePath, spark.sparkContext().hadoopConfiguration());
			});
	}
}
