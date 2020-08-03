
package eu.dnetlib.dhp.oa.graph.dump.community;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommunitySplitS3 implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(CommunitySplitS3.class);
	public void run(Boolean isSparkSessionManaged, String inputPath, String outputPath, String communityMapPath) {
		// public void run(Boolean isSparkSessionManaged, String inputPath, String outputPath, CommunityMap
		// communityMap) {
		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				SparkContext sc = spark.sparkContext();
				sc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
				sc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", "AK0MM6C2BYA0K1PNJYYX");
				sc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", "fpeiqUUpKAUOtO6JWMWLTxxlSxJ+yGYwHozm3jHK");
				execSplit(spark, inputPath, outputPath, communityMapPath); // communityMap.keySet());// ,
				// inputClazz);
				// execSplit(spark, inputPath, outputPath, communityMap.keySet());
			});
	}

	private static void execSplit(SparkSession spark, String inputPath, String outputPath,
		String communityMapPath) {
		// Set<String> communities) {

		Set<String> communities = Utils.getCommunityMap(spark, communityMapPath).keySet();

		Dataset<CommunityResult> result = Utils
			.readPath(spark, inputPath + "/publication", CommunityResult.class)
			.union(Utils.readPath(spark, inputPath + "/dataset", CommunityResult.class))
			.union(Utils.readPath(spark, inputPath + "/orp", CommunityResult.class))
			.union(Utils.readPath(spark, inputPath + "/software", CommunityResult.class));

		communities
			.stream()
			.forEach(c -> printResult(c, result, outputPath));

	}

	private static void printResult(String c, Dataset<CommunityResult> result, String outputPath) {
		Dataset<CommunityResult> community_products = result
			.filter(r -> containsCommunity(r, c));

		if (community_products.count() > 0) {
			log.info("Writing dump for community: {} ", c);
			community_products
				.repartition(1)
				.write()
				.option("compression", "gzip")
				.mode(SaveMode.Overwrite)
				.json(outputPath + "/" + c);
		}

	}

	private static boolean containsCommunity(CommunityResult r, String c) {
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
