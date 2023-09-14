
package eu.dnetlib.dhp.swh;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.*;

/**
 * Creates action sets for Crossref affiliation relations inferred by BIP!
 */
public class CollectSoftwareRepositoryURLs implements Serializable {

	private static final Logger log = LoggerFactory.getLogger(CollectSoftwareRepositoryURLs.class);
	// public static final String BIP_AFFILIATIONS_CLASSID = "result:organization:bipinference";
//    public static final String BIP_AFFILIATIONS_CLASSNAME = "Affiliation relation inferred by BIP!";
//    public static final String BIP_INFERENCE_PROVENANCE = "bip:affiliation:crossref";
	private static final String DEFAULT_VISIT_TYPE = "git";
	private static final int CONCURRENT_API_CALLS = 1;

	private static final String SWH_LATEST_VISIT_URL = "https://archive.softwareheritage.org/api/1/origin/%s/visit/latest/";

	public static <I extends Result> void main(String[] args) throws Exception {

		String jsonConfiguration = IOUtils
			.toString(
				CollectSoftwareRepositoryURLs.class
					.getResourceAsStream("/eu/dnetlib/dhp/swh/input_parameters.json"));

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
		parser.parseArgument(args);

		final Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

		final String hiveDbName = parser.get("hiveDbName");
		log.info("hiveDbName {}: ", hiveDbName);

		final String outputPath = parser.get("softwareCodeRepositoryURLs");
		log.info("softwareCodeRepositoryURLs {}: ", outputPath);

		final String hiveMetastoreUris = parser.get("hiveMetastoreUris");
		log.info("hiveMetastoreUris: {}", hiveMetastoreUris);

		SparkConf conf = new SparkConf();
		conf.set("hive.metastore.uris", hiveMetastoreUris);

		runWithSparkHiveSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				doRun(spark, hiveDbName, outputPath);
			});
	}

	private static <I extends Result> void doRun(SparkSession spark, String hiveDbName, String outputPath) {

		String queryTemplate = "SELECT distinct coderepositoryurl.value " +
			"FROM %s.software " +
			"WHERE coderepositoryurl.value IS NOT NULL";
		String query = String.format(queryTemplate, hiveDbName);

		log.info("Hive query to fetch software code URLs: {}", query);

		Dataset<Row> df = spark.sql(query);

		// write distinct repository URLs
		df
			.write()
			.mode(SaveMode.Overwrite)
//			.option("compression", "gzip")
			.csv(outputPath);
	}

	private static Dataset<Row> readSoftware(SparkSession spark, String inputPath) {
		return spark
			.read()
			.json(inputPath)
			.select(
				new Column("codeRepositoryUrl.value").as("codeRepositoryUrl"),
				new Column("dataInfo.deletedbyinference"),
				new Column("dataInfo.invisible"));
	}

	private static Dataset<Row> filterSoftware(Dataset<Row> softwareDF, Integer limit) {

		Dataset<Row> df = softwareDF
			.where(softwareDF.col("codeRepositoryUrl").isNotNull())
			.where("deletedbyinference = false")
			.where("invisible = false")
			.drop("deletedbyinference")
			.drop("invisible");

//        TODO remove when done
		df = df.limit(limit);

		return df;
	}

	public static Dataset<Row> makeParallelRequests(SparkSession spark, Dataset<Row> softwareDF) {
		// TODO replace with coalesce ?
		Dataset<Row> df = softwareDF.repartition(CONCURRENT_API_CALLS);

		log.info("Number of partitions: {}", df.rdd().getNumPartitions());

		ObjectMapper objectMapper = new ObjectMapper();

		List<Row> collectedRows = df
			.javaRDD()
			// max parallelism should be equal to the number of partitions here
			.mapPartitions((FlatMapFunction<Iterator<Row>, Row>) partition -> {
				List<Row> resultRows = new ArrayList<>();
				while (partition.hasNext()) {
					Row row = partition.next();
					String url = String.format(SWH_LATEST_VISIT_URL, row.getString(0));

//					String snapshotId = null;
//					String type = null;
//					String date = null;

					String responseBody = makeAPICall(url);
					TimeUnit.SECONDS.sleep(1);
//					Thread.sleep(500);
//					if (responseBody != null) {
//						LastVisitResponse visitResponse = objectMapper.readValue(responseBody, LastVisitResponse.class);
//						snapshotId = visitResponse.getSnapshot();
//						type = visitResponse.getType();
//						date = visitResponse.getDate();
//					}
//					resultRows.add(RowFactory.create(url, snapshotId, type, date));

					resultRows.add(RowFactory.create(url, responseBody));
				}
				return resultRows.iterator();

			})
			.collect();

		StructType resultSchema = new StructType()
			.add("codeRepositoryUrl", DataTypes.StringType)
			.add("response", DataTypes.StringType);

//			.add("snapshotId", DataTypes.StringType)
//			.add("type", DataTypes.StringType)
//			.add("date", DataTypes.StringType);

		// create a DataFrame from the collected rows
		return spark.createDataFrame(collectedRows, resultSchema);
	}

	private static String makeAPICall(String url) throws IOException {
		System.out.println(java.time.LocalDateTime.now());

		try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
			HttpGet httpGet = new HttpGet(url);
			httpGet
				.setHeader(
					"Authorization",
					"Bearer eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJhMTMxYTQ1My1hM2IyLTQwMTUtODQ2Ny05MzAyZjk3MTFkOGEifQ.eyJpYXQiOjE2OTQ2MzYwMjAsImp0aSI6IjkwZjdkNTNjLTQ5YTktNGFiMy1hY2E0LTcwMTViMjEyZTNjNiIsImlzcyI6Imh0dHBzOi8vYXV0aC5zb2Z0d2FyZWhlcml0YWdlLm9yZy9hdXRoL3JlYWxtcy9Tb2Z0d2FyZUhlcml0YWdlIiwiYXVkIjoiaHR0cHM6Ly9hdXRoLnNvZnR3YXJlaGVyaXRhZ2Uub3JnL2F1dGgvcmVhbG1zL1NvZnR3YXJlSGVyaXRhZ2UiLCJzdWIiOiIzMTY5OWZkNC0xNmE0LTQxOWItYTdhMi00NjI5MDY4ZjI3OWEiLCJ0eXAiOiJPZmZsaW5lIiwiYXpwIjoic3doLXdlYiIsInNlc3Npb25fc3RhdGUiOiIzMjYzMzEwMS00ZDRkLTQwMjItODU2NC1iMzNlMTJiNTE3ZDkiLCJzY29wZSI6Im9wZW5pZCBvZmZsaW5lX2FjY2VzcyBwcm9maWxlIGVtYWlsIn0.XHj1VIZu1dZ4Ej32-oU84mFmaox9cLNjXosNxwZM0Xs");
			try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
				int statusCode = response.getStatusLine().getStatusCode();
//				if (statusCode != 200)
//					return null;
				Header[] headers = response.getHeaders("X-RateLimit-Remaining");
				for (Header header : headers) {
					System.out
						.println(
							"Key : " + header.getName()
								+ " ,Value : " + header.getValue());
				}
				HttpEntity entity = response.getEntity();
				if (entity != null) {
					return EntityUtils.toString(entity);
				}
			}
		}
		return null;
	}
}
