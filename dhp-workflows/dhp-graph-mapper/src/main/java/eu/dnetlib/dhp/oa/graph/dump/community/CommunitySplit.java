
package eu.dnetlib.dhp.oa.graph.dump.community;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;

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
				execSplit(spark, inputPath, outputPath, Utils.getCommunityMap(spark, communityMapPath).keySet());
			});
	}

	private static void execSplit(SparkSession spark, String inputPath, String outputPath,
		Set<String> communities) {

		Dataset<CommunityResult> result = Utils
			.readPath(spark, inputPath + "/publication", CommunityResult.class)
			.union(Utils.readPath(spark, inputPath + "/dataset", CommunityResult.class))
			.union(Utils.readPath(spark, inputPath + "/orp", CommunityResult.class))
			.union(Utils.readPath(spark, inputPath + "/software", CommunityResult.class));

		communities
			.forEach(c -> printResult(c, result, outputPath));

	}

	private static void printResult(String community, Dataset<CommunityResult> result, String outputPath) {
		Dataset<CommunityResult> communityProducts = result
			.filter((FilterFunction<CommunityResult>) r -> containsCommunity(r, community));

		try {
			communityProducts.first();
			communityProducts
				.write()
				.option("compression", "gzip")
				.mode(SaveMode.Overwrite)
				.json(outputPath + "/" + community);
		} catch (NoSuchElementException e) {
			// ignoring it on purpose
		}
	}

	private static boolean containsCommunity(CommunityResult r, String community) {
		if (Optional.ofNullable(r.getContext()).isPresent()) {
			return !r
				.getContext()
				.stream()
				.filter(con -> con.getCode().equals(community))
				.collect(Collectors.toList())
				.isEmpty();
		}
		return false;
	}
}
