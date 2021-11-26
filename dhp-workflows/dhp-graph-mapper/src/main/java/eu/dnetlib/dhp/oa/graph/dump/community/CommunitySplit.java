
package eu.dnetlib.dhp.oa.graph.dump.community;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;
import eu.dnetlib.dhp.schema.dump.oaf.community.Context;

/**
 * This class splits the dumped results according to the research community - research initiative/infrastructure they
 * are related to. The information about the community is found in the element "context.id" in the result. Since the
 * context that can be found in the result can be associated not only to communities, a community Map is provided. It
 * will guide the splitting process. Note: the repartition(1) just before writing the results related to a community.
 * This is a choice due to uploading constraints (just one file for each community) As soon as a better solution will be
 * in place remove the repartition
 */
public class CommunitySplit implements Serializable {

	public void run(Boolean isSparkSessionManaged, String inputPath, String outputPath, String communityMapPath) {
		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				Utils.removeOutputDir(spark, outputPath);
				CommunityMap communityMap = Utils.getCommunityMap(spark, communityMapPath);
				execSplit(spark, inputPath, outputPath, communityMap);
			});
	}

	private static void execSplit(SparkSession spark, String inputPath, String outputPath,
		CommunityMap communities) {

		Dataset<CommunityResult> result = Utils
			.readPath(spark, inputPath + "/publication", CommunityResult.class)
			.union(Utils.readPath(spark, inputPath + "/dataset", CommunityResult.class))
			.union(Utils.readPath(spark, inputPath + "/orp", CommunityResult.class))
			.union(Utils.readPath(spark, inputPath + "/software", CommunityResult.class));

		communities
			.keySet()
			.stream()
			.forEach(c -> printResult(c, result, outputPath + "/" + communities.get(c).replace(" ", "_")));

	}

	private static void printResult(String c, Dataset<CommunityResult> result, String outputPath) {
		Dataset<CommunityResult> communityProducts = result
			.filter((FilterFunction<CommunityResult>) r -> containsCommunity(r, c));

		communityProducts
			.write()
			.option("compression", "gzip")
			.mode(SaveMode.Overwrite)
			.json(outputPath);

	}

	private static boolean containsCommunity(CommunityResult r, String c) {
		if (Optional.ofNullable(r.getContext()).isPresent()) {
			return r
				.getContext()
				.stream()
				.map(Context::getCode)
				.collect(Collectors.toList())
				.contains(c);
		}
		return false;
	}
}
