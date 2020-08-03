
package eu.dnetlib.dhp.oa.graph.dump.community;

import java.io.Serializable;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.oa.graph.dump.QueryInformationSystem;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class SparkSplitForCommunityS3 implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkSplitForCommunityS3.class);

	public static void main(String[] args) throws Exception {
		String jsonConfiguration = IOUtils
			.toString(
				SparkSplitForCommunityS3.class
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

		final String isLookUpUrl = parser.get("isLookUpUrl");
		log.info("isLookUpUrl: {}", isLookUpUrl);

		CommunitySplitS3 split = new CommunitySplitS3();

		// CommunityMap communityMap;

//		QueryInformationSystem queryInformationSystem = new QueryInformationSystem();
//		queryInformationSystem.setIsLookUp(getIsLookUpService(isLookUpUrl));
//		communityMap = queryInformationSystem.getCommunityMap();

		split.run(isSparkSessionManaged, inputPath, outputPath, communityMapPath);
		// split.run(isSparkSessionManaged, inputPath, outputPath, communityMap);

	}

	public static ISLookUpService getIsLookUpService(String isLookUpUrl) {
		return ISLookupClientFactory.getLookUpService(isLookUpUrl);
	}

}
