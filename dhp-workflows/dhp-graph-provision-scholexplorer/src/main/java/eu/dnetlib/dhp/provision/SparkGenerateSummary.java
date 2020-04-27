
package eu.dnetlib.dhp.provision;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.provision.scholix.summary.ScholixSummary;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkGenerateSummary {

	private static final String jsonIDPath = "$.id";

	public static void main(String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkGenerateSummary.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/provision/input_generate_summary_parameters.json")));
		parser.parseArgument(args);
		final SparkSession spark = SparkSession
			.builder()
			.appName(SparkExtractRelationCount.class.getSimpleName())
			.master(parser.get("master"))
			.getOrCreate();

		final String graphPath = parser.get("graphPath");
		final String workingDirPath = parser.get("workingDirPath");

		final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		Dataset<RelatedItemInfo> rInfo = spark
			.read()
			.load(workingDirPath + "/relatedItemCount")
			.as(Encoders.bean(RelatedItemInfo.class));

		Dataset<ScholixSummary> entity = spark
			.createDataset(
				sc
					.textFile(
						graphPath + "/publication," + graphPath + "/dataset," + graphPath + "/unknown")
					.map(
						s -> ScholixSummary
							.fromJsonOAF(
								ProvisionUtil.getItemTypeFromId(DHPUtils.getJPathString(jsonIDPath, s)),
								s))
					.rdd(),
				Encoders.bean(ScholixSummary.class));

		Dataset<ScholixSummary> summaryComplete = rInfo
			.joinWith(entity, rInfo.col("source").equalTo(entity.col("id")))
			.map(
				(MapFunction<Tuple2<RelatedItemInfo, ScholixSummary>, ScholixSummary>) t -> {
					ScholixSummary scholixSummary = t._2();
					RelatedItemInfo relatedItemInfo = t._1();
					scholixSummary.setRelatedDatasets(relatedItemInfo.getRelatedDataset());
					scholixSummary
						.setRelatedPublications(
							relatedItemInfo.getRelatedPublication());
					scholixSummary.setRelatedUnknown(relatedItemInfo.getRelatedUnknown());
					return scholixSummary;
				},
				Encoders.bean(ScholixSummary.class));

		summaryComplete.write().save(workingDirPath + "/summary");

		// JavaPairRDD<String, String> relationCount =
		// sc.textFile(workingDirPath+"/relatedItemCount").mapToPair((PairFunction<String, String,
		// String>) i -> new Tuple2<>(DHPUtils.getJPathString(jsonIDPath, i), i));
		//
		// JavaPairRDD<String, String> entities =
		// sc.textFile(graphPath + "/publication")
		// .filter(ProvisionUtil::isNotDeleted)
		// .mapToPair((PairFunction<String, String, String>) i -> new
		// Tuple2<>(DHPUtils.getJPathString(jsonIDPath, i), i))
		// .union(
		// sc.textFile(graphPath + "/dataset")
		// .filter(ProvisionUtil::isNotDeleted)
		// .mapToPair((PairFunction<String, String, String>)
		// i ->
		// new Tuple2<>(DHPUtils.getJPathString(jsonIDPath, i), i))
		// )
		// .union(
		// sc.textFile(graphPath + "/unknown")
		// .filter(ProvisionUtil::isNotDeleted)
		// .mapToPair((PairFunction<String, String, String>)
		// i ->
		// new Tuple2<>(DHPUtils.getJPathString(jsonIDPath, i), i))
		// );
		// entities.join(relationCount).map((Function<Tuple2<String, Tuple2<String, String>>,
		// String>) k ->
		// ScholixSummary.fromJsonOAF(ProvisionUtil.getItemTypeFromId(k._1()),
		// k._2()._1(), k._2()._2())).saveAsTextFile(workingDirPath+"/summary", GzipCodec.class);
		//
		//
		// ;

	}
}
