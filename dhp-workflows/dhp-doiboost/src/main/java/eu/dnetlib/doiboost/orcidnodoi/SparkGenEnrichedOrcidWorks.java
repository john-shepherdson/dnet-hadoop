
package eu.dnetlib.doiboost.orcidnodoi;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.doiboost.orcid.model.AuthorData;
import eu.dnetlib.doiboost.orcidnodoi.model.WorkDataNoDoi;
import eu.dnetlib.doiboost.orcidnodoi.similarity.AuthorMatcher;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class SparkGenEnrichedOrcidWorks {

	public static void main(String[] args) throws IOException, Exception {
		Logger logger = LoggerFactory.getLogger(SparkGenEnrichedOrcidWorks.class);
		logger.info("[ SparkGenerateDoiAuthorList STARTED]");

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkGenEnrichedOrcidWorks.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/gen_enriched_orcid_works_parameters.json")));
		parser.parseArgument(args);
		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		logger.info("isSparkSessionManaged: {}", isSparkSessionManaged);
		final String workingPath = parser.get("workingPath");
		logger.info("workingPath: ", workingPath);
		final String outputEnrichedWorksPath = parser.get("outputEnrichedWorksPath");
		logger.info("outputEnrichedWorksPath: ", outputEnrichedWorksPath);
		final String outputWorksPath = parser.get("outputWorksPath");
		logger.info("outputWorksPath: ", outputWorksPath);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

				JavaPairRDD<Text, Text> summariesRDD = sc
					.sequenceFile(workingPath + "../orcid_summaries/output/authors.seq", Text.class, Text.class);
				Dataset<AuthorData> summariesDataset = spark
					.createDataset(
						summariesRDD.map(seq -> loadAuthorFromJson(seq._1(), seq._2())).rdd(),
						Encoders.bean(AuthorData.class));

				JavaPairRDD<Text, Text> activitiesRDD = sc
					.sequenceFile(workingPath + outputWorksPath + "works_X.seq" , Text.class, Text.class);
				Dataset<WorkDataNoDoi> activitiesDataset = spark
					.createDataset(
						activitiesRDD.map(seq -> loadWorkFromJson(seq._1(), seq._2())).rdd(),
						Encoders.bean(WorkDataNoDoi.class));

				activitiesDataset
						.joinWith(
								summariesDataset,
								activitiesDataset.col("oid").equalTo(summariesDataset.col("oid")), "inner")
						.map(
								(MapFunction<Tuple2<WorkDataNoDoi, AuthorData>, Tuple2<String, WorkDataNoDoi>>) value -> {
									WorkDataNoDoi w = value._1;
									AuthorData a = value._2;
									AuthorMatcher.match(a, w.getContributors());
									return new Tuple2<>(a.getOid(), w);
								},
								Encoders.tuple(Encoders.STRING(), Encoders.bean(WorkDataNoDoi.class)))
						.filter(Objects::nonNull)
						.toJavaRDD()
						.saveAsTextFile(workingPath + outputEnrichedWorksPath);;
			});
	}

	private static AuthorData loadAuthorFromJson(Text orcidId, Text json) {
		AuthorData authorData = new AuthorData();
		authorData.setOid(orcidId.toString());
		JsonElement jElement = new JsonParser().parse(json.toString());
		authorData.setName(getJsonValue(jElement, "name"));
		authorData.setSurname(getJsonValue(jElement, "surname"));
		authorData.setCreditName(getJsonValue(jElement, "creditname"));
		return authorData;
	}

	private static WorkDataNoDoi loadWorkFromJson(Text orcidId, Text json) {
		WorkDataNoDoi workData = new Gson().fromJson(json.toString(), WorkDataNoDoi.class);
		return workData;
	}

	private static String getJsonValue(JsonElement jElement, String property) {
		if (jElement.getAsJsonObject().has(property)) {
			JsonElement name = null;
			name = jElement.getAsJsonObject().get(property);
			if (name != null && !name.isJsonNull()) {
				return name.getAsString();
			}
		}
		return null;
	}
}
