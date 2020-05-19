
package eu.dnetlib.doiboost.orcid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.util.LongAccumulator;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.scholexplorer.DLIRelation;
import eu.dnetlib.doiboost.orcid.model.AuthorData;
import eu.dnetlib.doiboost.orcid.model.DownloadedRecordData;
import eu.dnetlib.doiboost.orcid.model.WorkData;
import scala.Tuple2;

public class SparkGenerateDoiAuthorList {

	public static void main(String[] args) throws IOException, Exception {
		Logger logger = LoggerFactory.getLogger(SparkGenerateDoiAuthorList.class);
		logger.info("[ SparkGenerateDoiAuthorList STARTED]");

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkGenerateDoiAuthorList.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/gen_doi_author_list_orcid_parameters.json")));
		parser.parseArgument(args);
		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		logger.info("isSparkSessionManaged: {}", isSparkSessionManaged);
		final String workingPath = parser.get("workingPath");
		logger.info("workingPath: ", workingPath);
		final String outputDoiAuthorListPath = parser.get("outputDoiAuthorListPath");
		logger.info("outputDoiAuthorListPath: ", outputDoiAuthorListPath);
		

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

				JavaPairRDD<Text, Text> summariesRDD = sc.sequenceFile(workingPath + "../orcid_summaries/output/authors.seq", Text.class, Text.class);
				Dataset<AuthorData> summariesDataset = spark
				.createDataset(summariesRDD.map(seq -> loadAuthorFromJson(seq._1(), seq._2())).rdd(), 
						Encoders.bean(AuthorData.class));
				
				JavaPairRDD<Text, Text> activitiesRDD = sc.sequenceFile(workingPath + "/output/*.seq", Text.class, Text.class);
				Dataset<WorkData> activitiesDataset = spark
						.createDataset(activitiesRDD.map(seq -> loadWorkFromJson(seq._1(), seq._2())).rdd(), 
								Encoders.bean(WorkData.class));

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
	
	private static WorkData loadWorkFromJson(Text orcidId, Text json) {
		WorkData workData = new WorkData();
		workData.setOid(orcidId.toString());
		JsonElement jElement = new JsonParser().parse(json.toString());
		workData.setDoi(getJsonValue(jElement, "doi"));
		return workData;
	}
	
	private static String getJsonValue(JsonElement jElement, String property) {
		if (jElement.getAsJsonObject().has(property)) {
			JsonElement name = null;
			name = jElement.getAsJsonObject().get(property);
			if (name != null && name.isJsonObject()) {
				return name.getAsString();
			}
		}
		return null;
	}
}
