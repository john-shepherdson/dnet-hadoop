
package eu.dnetlib.dhp.oa.graph.dump.community;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

/**
 * Spark job to trigger the split of results associated to research community - reseach initiative/infrasctructure. The
 * actual split is performed by the class CommunitySplit
 */
public class SparkSplitForCommunity implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkSplitForCommunity.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkSplitForCommunity.class
					.getResourceAsStream(
						"/eu/dnetlib/dhp/oa/graph/dump/split_parameters.json"));

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

		final String communityMapPath = parser.get("communityMapPath");

		CommunitySplit split = new CommunitySplit();
		split.run(isSparkSessionManaged, inputPath, outputPath, communityMapPath);

	}

}
