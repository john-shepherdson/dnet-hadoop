
package eu.dnetlib.dhp.oa.graph.dump.graph;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.QueryInformationSystem;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.schema.oaf.Result;

/**
 * Spark job that fires the extraction of relations from entities
 */
public class SparkExtractRelationFromEntities implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(SparkExtractRelationFromEntities.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkExtractRelationFromEntities.class
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

		final String communityMapPath = parser.get("communityMapPath");

		Class<? extends Result> inputClazz = (Class<? extends Result>) Class.forName(resultClassName);

		Extractor extractor = new Extractor();
		extractor.run(isSparkSessionManaged, inputPath, outputPath, inputClazz, communityMapPath);

	}

}
