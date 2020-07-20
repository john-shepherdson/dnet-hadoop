
package eu.dnetlib.dhp.oa.graph.dump.community;

import java.io.Serializable;
import java.util.*;

import eu.dnetlib.dhp.oa.graph.dump.DumpProducts;
import eu.dnetlib.dhp.oa.graph.dump.QueryInformationSystem;
import eu.dnetlib.dhp.oa.graph.dump.Utils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

public class SparkDumpCommunityProducts implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(SparkDumpCommunityProducts.class);
	private static QueryInformationSystem queryInformationSystem;

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

		final String isLookUpUrl = parser.get("isLookUpUrl");
		log.info("isLookUpUrl: {}", isLookUpUrl);

		final Optional<String> cm = Optional.ofNullable(parser.get("communityMap"));

		Class<? extends Result> inputClazz = (Class<? extends Result>) Class.forName(resultClassName);

		queryInformationSystem = new QueryInformationSystem();
		queryInformationSystem.setIsLookUp(Utils.getIsLookUpService(isLookUpUrl));
		CommunityMap communityMap = queryInformationSystem.getCommunityMap();

		DumpProducts dump = new DumpProducts();

		dump.run(isSparkSessionManaged, inputPath, outputPath, communityMap, inputClazz, false);

	}




}
