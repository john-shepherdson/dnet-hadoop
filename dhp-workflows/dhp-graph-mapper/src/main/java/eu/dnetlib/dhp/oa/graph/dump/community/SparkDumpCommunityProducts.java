/**
 *Spark action to trigger the dump of results associated to research community - reseach initiative/infrasctructure
 * The actual dump if performed via the class DumpProducts that is used also for the entire graph dump
 */

package eu.dnetlib.dhp.oa.graph.dump.community;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.DumpProducts;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;
import eu.dnetlib.dhp.schema.oaf.Result;

public class SparkDumpCommunityProducts implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkDumpCommunityProducts.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkDumpCommunityProducts.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/input_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String inputPath = parser.get("sourcePath");
		log.info("inputPath: {}", inputPath);

		final String outputPath = parser.get("outputPath");
		log.info("outputPath: {}", outputPath);

		final String resultClassName = parser.get("resultTableName");
		log.info("resultTableName: {}", resultClassName);

		String communityMapPath = parser.get("communityMapPath");

		Class<? extends Result> inputClazz = (Class<? extends Result>) Class.forName(resultClassName);

		DumpProducts dump = new DumpProducts();

		dump
			.run(
				isSparkSessionManaged, inputPath, outputPath, communityMapPath, inputClazz, CommunityResult.class,
				false);

	}

}
