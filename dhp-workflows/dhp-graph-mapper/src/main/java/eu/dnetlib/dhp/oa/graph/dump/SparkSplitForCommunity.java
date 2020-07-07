
package eu.dnetlib.dhp.oa.graph.dump;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.dump.oaf.Result;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;

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

//        final String resultClassName = parser.get("resultTableName");
//        log.info("resultTableName: {}", resultClassName);

		final String isLookUpUrl = parser.get("isLookUpUrl");
		log.info("isLookUpUrl: {}", isLookUpUrl);

		final Optional<String> cm = Optional.ofNullable(parser.get("communityMap"));

		SparkConf conf = new SparkConf();

		CommunityMap communityMap;

		if (!isLookUpUrl.equals("BASEURL:8280/is/services/isLookUp")) {
			QueryInformationSystem queryInformationSystem = new QueryInformationSystem();
			queryInformationSystem.setIsLookUp(getIsLookUpService(isLookUpUrl));
			communityMap = queryInformationSystem.getCommunityMap();
		} else {
			communityMap = new Gson().fromJson(cm.get(), CommunityMap.class);
		}

		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				execSplit(spark, inputPath, outputPath, communityMap.keySet());// , inputClazz);
			});
	}

	public static ISLookUpService getIsLookUpService(String isLookUpUrl) {
		return ISLookupClientFactory.getLookUpService(isLookUpUrl);
	}

	private static void execSplit(SparkSession spark, String inputPath, String outputPath,
		Set<String> communities) {// }, Class<R> inputClazz) {

		Dataset<Result> result = Utils
			.readPath(spark, inputPath + "/publication", Result.class)
			.union(Utils.readPath(spark, inputPath + "/dataset", Result.class))
			.union(Utils.readPath(spark, inputPath + "/orp", Result.class))
			.union(Utils.readPath(spark, inputPath + "/software", Result.class));

		communities
			.stream()
			.forEach(c -> printResult(c, result, outputPath));

	}

	private static void printResult(String c, Dataset<Result> result, String outputPath) {
		Dataset<Result> community_products = result
				.filter(r -> containsCommunity(r, c));

 			if(community_products.count() > 0){
			community_products.repartition(1)
					.write()
					.option("compression", "gzip")
					.mode(SaveMode.Overwrite)
					.json(outputPath + "/" + c);
		}

	}

	private static boolean containsCommunity(Result r, String c) {
		if (Optional.ofNullable(r.getContext()).isPresent()) {
			return r
				.getContext()
				.stream()
				.filter(con -> con.getCode().equals(c))
				.collect(Collectors.toList())
				.size() > 0;
		}
		return false;
	}
}
