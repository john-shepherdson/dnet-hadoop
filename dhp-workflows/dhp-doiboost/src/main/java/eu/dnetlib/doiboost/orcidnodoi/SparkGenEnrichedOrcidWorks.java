
package eu.dnetlib.doiboost.orcidnodoi;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.orcid.AuthorData;
import eu.dnetlib.doiboost.orcid.json.JsonHelper;
import eu.dnetlib.doiboost.orcidnodoi.model.WorkDataNoDoi;
import eu.dnetlib.doiboost.orcidnodoi.oaf.PublicationToOaf;
import eu.dnetlib.doiboost.orcidnodoi.similarity.AuthorMatcher;
import scala.Tuple2;

/**
 * This spark job generates orcid publications no doi dataset
 */

public class SparkGenEnrichedOrcidWorks {

	static Logger logger = LoggerFactory.getLogger(SparkGenEnrichedOrcidWorks.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws IOException, Exception {

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
		final String workingPath = parser.get("workingPath");
		final String outputEnrichedWorksPath = parser.get("outputEnrichedWorksPath");
		final String outputWorksPath = parser.get("outputWorksPath");
		final String hdfsServerUri = parser.get("hdfsServerUri");

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

				JavaPairRDD<Text, Text> summariesRDD = sc
					.sequenceFile(workingPath + "authors/authors.seq", Text.class, Text.class);
				Dataset<AuthorData> summariesDataset = spark
					.createDataset(
						summariesRDD.map(seq -> loadAuthorFromJson(seq._1(), seq._2())).rdd(),
						Encoders.bean(AuthorData.class));
				logger.info("Authors data loaded: " + summariesDataset.count());

				JavaPairRDD<Text, Text> activitiesRDD = sc
					.sequenceFile(workingPath + outputWorksPath + "*.seq", Text.class, Text.class);
				Dataset<WorkDataNoDoi> activitiesDataset = spark
					.createDataset(
						activitiesRDD.map(seq -> loadWorkFromJson(seq._1(), seq._2())).rdd(),
						Encoders.bean(WorkDataNoDoi.class));
				logger.info("Works data loaded: " + activitiesDataset.count());

				JavaRDD<Tuple2<String, String>> enrichedWorksRDD = activitiesDataset
					.joinWith(
						summariesDataset,
						activitiesDataset.col("oid").equalTo(summariesDataset.col("oid")), "inner")
					.map(
						(MapFunction<Tuple2<WorkDataNoDoi, AuthorData>, Tuple2<String, String>>) value -> {
							WorkDataNoDoi w = value._1;
							AuthorData a = value._2;
							AuthorMatcher.match(a, w.getContributors());
							return new Tuple2<>(a.getOid(), JsonHelper.createOidWork(w));
						},
						Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
					.filter(Objects::nonNull)
					.toJavaRDD();
				logger.info("Enriched works RDD ready.");

				final LongAccumulator parsedPublications = spark.sparkContext().longAccumulator("parsedPublications");
				final LongAccumulator enrichedPublications = spark
					.sparkContext()
					.longAccumulator("enrichedPublications");
				final LongAccumulator errorsGeneric = spark.sparkContext().longAccumulator("errorsGeneric");
				final LongAccumulator errorsInvalidTitle = spark.sparkContext().longAccumulator("errorsInvalidTitle");
				final LongAccumulator errorsNotFoundAuthors = spark
					.sparkContext()
					.longAccumulator("errorsNotFoundAuthors");
				final LongAccumulator errorsInvalidType = spark.sparkContext().longAccumulator("errorsInvalidType");
				final PublicationToOaf publicationToOaf = new PublicationToOaf(
					parsedPublications,
					enrichedPublications,
					errorsGeneric,
					errorsInvalidTitle,
					errorsNotFoundAuthors,
					errorsInvalidType);
				JavaRDD<Publication> oafPublicationRDD = enrichedWorksRDD
					.map(
						e -> {
							return (Publication) publicationToOaf
								.generatePublicationActionsFromJson(e._2());
						})
					.filter(p -> p != null);

				sc.hadoopConfiguration().set("mapreduce.output.fileoutputformat.compress", "true");

				oafPublicationRDD
					.mapToPair(
						p -> new Tuple2<>(p.getClass().toString(),
							OBJECT_MAPPER.writeValueAsString(new AtomicAction<>(Publication.class, (Publication) p))))
					.mapToPair(t -> new Tuple2(new Text(t._1()), new Text(t._2())))
					.saveAsNewAPIHadoopFile(
						workingPath.concat(outputEnrichedWorksPath),
						Text.class,
						Text.class,
						SequenceFileOutputFormat.class,
						sc.hadoopConfiguration());

				logger.info("parsedPublications: " + parsedPublications.value().toString());
				logger.info("enrichedPublications: " + enrichedPublications.value().toString());
				logger.info("errorsGeneric: " + errorsGeneric.value().toString());
				logger.info("errorsInvalidTitle: " + errorsInvalidTitle.value().toString());
				logger.info("errorsNotFoundAuthors: " + errorsNotFoundAuthors.value().toString());
				logger.info("errorsInvalidType: " + errorsInvalidType.value().toString());
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
		return new String("");
	}
}
